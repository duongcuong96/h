# -*- coding: utf-8 -*-
from __future__ import unicode_literals
import logging
from collections import namedtuple
from contextlib import contextmanager

from elasticsearch1.exceptions import ConnectionTimeout

from h.celery import celery
from h.search import query

log = logging.getLogger(__name__)

SearchResult = namedtuple('SearchResult', [
    'total',
    'annotation_ids',
    'reply_ids',
    'aggregations'])


class Search(object):
    """
    Search is the primary way to initiate a search on the annotation index.

    :param request: the request object
    :type request: pyramid.request.Request

    :param separate_replies: Whether or not to return all replies to the
        annotations returned by this search. If this is True then the
        resulting annotations will only include top-level annotations, not replies.
    :type separate_replies: bool

    :param stats: An optional statsd client to which some metrics will be
        published.
    :type stats: statsd.client.StatsClient
    """
    def __init__(self, request, separate_replies=False, stats=None, _replies_limit=200):
        self.request = request
        self.es = request.es
        self.es6 = None
        self.separate_replies = separate_replies
        self.stats = stats
        self._replies_limit = _replies_limit

        self.builder = self._default_querybuilder(request, self.es)
        self.reply_builder = self._default_querybuilder(request, self.es)
        if celery.request.feature('search_es6'):
            self.es6 = request.es6
            self.builder6 = self._default_querybuilder(request, self.es6)
            self.reply_builder6 = self._default_querybuilder(request, self.es6)

    def run(self, params):
        """
        Execute the search query

        :param params: the search parameters
        :type params: dict-like

        :returns: The search results
        :rtype: SearchResult
        """
        if self.es6:
            return self._run(params, self.es6, self.builder6, self.reply_builder6)
        return self._run(params, self.es, self.builder, self.reply_builder)

    def _run(self, params, es, builder, reply_builder):
        total, annotation_ids, aggregations = self._search_annotations(params, es, builder)
        reply_ids = self._search_replies(annotation_ids, es, reply_builder)

        return SearchResult(total, annotation_ids, reply_ids, aggregations)

    def clear(self):
        """Clear search filters, aggregators, and matchers."""
        self.builder = query.Builder(es_version=self.es.version)
        self.reply_builder = query.Builder(es_version=self.es.version)
        if self.es6:
            self.builder6 = query.Builder(es_version=self.es6.version)
            self.reply_builder6 = query.Builder(es_version=self.es6.version)

    def append_filter(self, filter_):
        """Append a search filter to the annotation and reply query."""
        self.builder.append_filter(filter_)
        self.reply_builder.append_filter(filter_)
        if self.es6:
            self.builder6.append_filter(filter_)
            self.reply_builder6.append_filter(filter_)

    def append_matcher(self, matcher):
        """Append a search matcher to the annotation and reply query."""
        self.builder.append_matcher(matcher)
        self.reply_builder.append_matcher(matcher)
        if self.es6:
            self.builder6.append_matcher(matcher)
            self.reply_builder6.append_matcher(matcher)

    def append_aggregation(self, aggregation):
        self.builder.append_aggregation(aggregation)
        if self.es6:
            self.builder.append_aggregation(aggregation)

    def _search_annotations(self, params, es, builder):
        if self.separate_replies:
            self.builder.append_filter(query.TopLevelAnnotationsFilter())
            if self.es6:
                self.builder6.append_filter(query.TopLevelAnnotationsFilter())

        response = None
        with self._instrument():
            response = es.conn.search(index=es.index,
                                      doc_type=es.mapping_type,
                                      _source=False,
                                      body=builder.build(params))
        total = response['hits']['total']
        annotation_ids = [hit['_id'] for hit in response['hits']['hits']]
        aggregations = self._parse_aggregation_results(response.get('aggregations', None), builder)
        return (total, annotation_ids, aggregations)

    def _search_replies(self, annotation_ids, es, reply_builder):
        if not self.separate_replies:
            return []

        reply_builder.append_matcher(query.RepliesMatcher(annotation_ids))

        response = None
        with self._instrument():
            response = es.conn.search(
                index=es.index,
                doc_type=es.mapping_type,
                _source=False,
                body=reply_builder.build({'limit': self._replies_limit}))

        if len(response['hits']['hits']) < response['hits']['total']:
            log.warn("The number of reply annotations exceeded the page size "
                     "of the Elasticsearch query. We currently don't handle "
                     "this, our search API doesn't support pagination of the "
                     "reply set.")

        return [hit['_id'] for hit in response['hits']['hits']]

    def _parse_aggregation_results(self, aggregations, builder):
        if not aggregations:
            return {}

        results = {}
        for key, result in aggregations.items():
            for agg in builder.aggregations:
                if key != agg.key:
                    continue

                results[key] = agg.parse_result(result)
                break

        return results

    @contextmanager
    def _instrument(self):
        if not self.stats:
            yield
            return

        s = self.stats.pipeline()
        timer = s.timer('search.query').start()
        try:
            yield
            s.incr('search.query.success')
        except ConnectionTimeout:
            s.incr('search.query.timeout')
            raise
        except:  # noqa: E722
            s.incr('search.query.error')
            raise
        finally:
            timer.stop()
            s.send()

    @staticmethod
    def _default_querybuilder(request, es):
        builder = query.Builder(es_version=es.version)
        builder.append_filter(query.DeletedFilter())
        builder.append_filter(query.AuthFilter(request))
        builder.append_filter(query.UriFilter(request))
        builder.append_filter(query.GroupFilter())
        builder.append_filter(query.GroupAuthFilter(request))
        builder.append_filter(query.UserFilter())
        builder.append_filter(query.NipsaFilter(request))
        builder.append_matcher(query.AnyMatcher())
        builder.append_matcher(query.TagsMatcher())
        return builder
