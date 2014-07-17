import logging
import shlex
import time

from django.db import models
from django.utils.encoding import smart_str, smart_unicode
from google.appengine.ext import db
from google.appengine.ext.deferred import defer
from django.conf import settings

"""
    REMAINING TO DO!

    1. Partial matches. These should be recorded as new Index instances, with an FK to the full term being indexes.
       Partials should only be recorded for between 4, and len(original_term) - 1 characters. Partial matches should be much more highly scored,
       the lower the match, the more the score should be
    2. Cross-join indexing  e.g. book__title on an Author.
    3. Field matches. e.g "id:1234 field1:banana". This should match any other words using indexes, but only return matches that match the field lookups
"""

QUEUE_FOR_INDEXING = getattr(settings, "QUEUE_FOR_INDEXING", "default")


class GlobalOccuranceCount(models.Model):
    id = models.CharField(max_length=1024, primary_key=True)
    count = models.PositiveIntegerField(default=0)

    def update(self):
        count = sum(Index.objects.filter(iexact=self.id).values_list('occurances', flat=True))

        @db.transactional
        def txn():
            goc = GlobalOccuranceCount.objects.get(pk=self.id)
            goc.count = count
            goc.save()

        while True:
            try:
                txn()
                break
            except db.TransactionFailedError:
                time.sleep(1)
                continue


class AbstractIndex(models.Model):
    iexact = models.CharField(max_length=1024)
    occurances = models.PositiveIntegerField(default=0)

    class Meta:
        abstract = True

    def index(self, defer_index=True):
        if db.is_in_transaction() or defer_index:
            defer(self.reindex, _queue=QUEUE_FOR_INDEXING)
        else:
            self.reindex

    def reindex(self):
        self.unindex()
        self._apply_index()

    def unindex(self, instance):
        indexes = self._get_indexes(instance)
        for index in indexes:

            @db.transactional(xg=True)
            def txn(_index):
                count = GlobalOccuranceCount.objects.get(pk=_index.iexact)
                count.count -= _index.occurances
                count.save()
                _index.delete()

            try:
                while True:
                    try:
                        txn(index)
                        break
                    except db.TransactionFailedError:
                        logging.warning("Transaction collision, retrying!")
                        time.sleep(1)
                        continue
            except GlobalOccuranceCount.DoesNotExist:
                logging.warning(
                    "A GlobalOccuranceCount for Index: %s "
                    "does not exist, ignoring", index.pk
                )
                continue

    def search(self, model_class, search_string, per_page=50, current_page=1, total_pages=10, **filters):
        terms = self.parse_terms(search_string)

        instance_weights = self._get_matches(model_class, terms)

        final_weights = []
        for k, v in instance_weights.items():
            """
                This is where we rank the results. Lower scores are better. Scores are based
                on the commonality of the word. More matches are rewarded, but not too much so
                that rarer terms still have a chance.

                Examples for n matches:

                1 = 1 + (0 * 0.5) = 1    -> scores / 1
                2 = 2 + (1 * 0.5) = 2.5  -> scores / 2.5 (rather than 2)
                3 = 3 + (2 * 0.5) = 4    -> scores / 4 (rather than 3)
            """

            n = float(len(v))
            final_weights.append((sum(v) / (n + ((n-1) * 0.5)), k))

        final_weights.sort()

        final_weights = final_weights[:total_pages*per_page]
        #Restrict to the page
        offset = ((current_page - 1) * per_page)
        final_weights = final_weights[offset:offset + per_page]

        order = {}
        for index, (score, pk) in enumerate(final_weights):
            order[pk] = index

        queryset = model_class.objects.all()
        if filters:
            queryset = queryset.filter(**filters)

        # Workaround for an obscure bug when using datastore_utils.CachingQuerySet
        # that returns no results when filtering by pk at this point.
        # Feel free to investigate and fix it if you have any insight.
        # results = queryset.filter(pk__in=order.keys())
        results = [r for r in queryset if r.pk in order.keys()]
        sorted_results = [None] * len(results)

        for result in results:
            position = order[result.pk]
            sorted_results[position] = result

        return sorted_results

    @classmethod
    def parse_terms(cls, search_string):
        return shlex.split(smart_str(search_string.lower()))

    def _get_indexes(self, instance):
        raise NotImplementedError("Subclasses should implement this.")

    def _get_matches(self):
        raise NotImplementedError("Subclasses should implement this.")

    def _get_data(self, field, instance):
        raise NotImplementedError("Subclasses should implement this.")

    def _apply_index(self, instance, fields_to_index):
        raise NotImplementedError("Subclasses should implement this.")

    def _weight_results(self, instance_weights):
        final_weights = []
        for k, v in instance_weights.items():
            """
                This is where we rank the results. Lower scores are better. Scores are based
                on the commonality of the word. More matches are rewarded, but not too much so
                that rarer terms still have a chance.

                Examples for n matches:

                1 = 1 + (0 * 0.5) = 1    -> scores / 1
                2 = 2 + (1 * 0.5) = 2.5  -> scores / 2.5 (rather than 2)
                3 = 3 + (2 * 0.5) = 4    -> scores / 4 (rather than 3)
            """

            n = float(len(v))
            final_weights.append((sum(v) / (n + ((n-1) * 0.5)), k))

        return final_weights.sort()

    def _apply_paging_to_results(self, final_weights, per_page, current_page, total_pages):
        #Restrict to the max possible
        final_weights = final_weights[:total_pages*per_page]

        #Restrict to the page
        offset = ((current_page - 1) * per_page)
        return final_weights[offset:offset + per_page]


class Index(AbstractIndex):
    instance_db_table = models.CharField(max_length=1024)
    instance_pk = models.PositiveIntegerField(default=0)

    class Meta:
        unique_together = [
            ('iexact', 'instance_db_table', 'instance_pk')
        ]

    def index(self, instance, fields_to_index, defer_index=True):
        if db.is_in_transaction() or defer_index:
            defer(self.reindex, instance, fields_to_index, _queue=QUEUE_FOR_INDEXING)
        else:
            self.reindex(instance, fields_to_index)

    def reindex(self, instance, fields_to_index):
        self.unindex(instance)
        self._apply_index(instance, fields_to_index)

    def _get_indexes(self, instance):
        return Index.objects.filter(
            instance_db_table=instance._meta.db_table, instance_pk=instance.pk).all()

    def _get_data(self, field, instance):
        lookups = field.split("__")
        value = instance
        for lookup in lookups:
            if value is None:
                continue
            value = getattr(value, lookup)

            if "RelatedManager" in value.__class__.__name__:
                if lookup == lookups[-2]:
                    return [getattr(x, lookups[-1]) for x in value.all()]
                else:
                    raise TypeError("You can only index one level of related object")

            elif hasattr(value, "__iter__"):
                if lookup == lookups[-1]:
                    return value
                else:
                    raise TypeError("You can only index one level of iterable")

        return [value]

    def _get_matches(self, model_class, terms):
        matching_terms = dict(GlobalOccuranceCount.objects.filter(pk__in=terms).values_list('pk', 'count'))
        matches = Index.objects.filter(iexact__in=terms, instance_db_table=model_class._meta.db_table).all()

        instance_weights = {}

        for match in matches:
            instance_weights.setdefault(match.instance_pk, []).append(matching_terms[match.iexact])

        return instance_weights

    def _apply_index(self, instance, fields_to_index):
        for field in fields_to_index:
            texts = self._get_data(field, instance)
            for text in texts:
                if text is None:
                    continue

                text = smart_unicode(text)
                text = text.lower()  # Normalize

                words = text.split(" ")  # Split on whitespace

                #Build up combinations of adjacent words
                for i in xrange(0, len(words)):
                    for j in xrange(1, 5):
                        term_words = words[i:i+j]

                        if len(term_words) != j:
                            break

                        term = u" ".join(term_words)

                        if not term.strip():
                            continue

                        @db.transactional(xg=True)
                        def txn(term_):
                            logging.info("Indexing: '%s', %s", term_, type(term_))
                            term_count = text.count(term_)

                            Index.objects.create(
                                iexact=term_,
                                instance_db_table=instance._meta.db_table,
                                instance_pk=instance.pk,
                                occurances=term_count
                            )
                            counter, created = GlobalOccuranceCount.objects.get_or_create(pk=term_)
                            counter.count += term_count
                            counter.save()

                        while True:
                            try:
                                txn(term)
                                break
                            except db.TransactionFailedError:
                                logging.warning("Transaction collision, retrying!")
                                time.sleep(1)
                                continue

from django.dispatch import receiver
from django.db.models.signals import post_save, pre_delete


@receiver(post_save)
def post_save_index(sender, instance, created, raw, *args, **kwargs):
    if getattr(instance, "Search", None):
        fields_to_index = getattr(instance.Search, "fields", [])
        if fields_to_index:
            instance.index(instance, fields_to_index, defer_index=not raw)  # Don't defer if we are loading from a fixture


@receiver(pre_delete)
def pre_delete_unindex(sender, instance, using, *args, **kwarg):
    if getattr(instance, "Search", None):
        instance.unindex()
