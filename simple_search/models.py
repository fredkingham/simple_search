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

def _do_index(instance, fields_to_index):
    def get_data_from_field(field_, instance_):
        lookups = field_.split("__")
        value = instance
        for lookup in lookups:
            if value is None:
                continue
            value = getattr(value, lookup)

            if "RelatedManager" in value.__class__.__name__:
                if lookup == lookups[-2]:
                    return [ getattr(x, lookups[-1]) for x in value.all() ]
                else:
                    raise TypeError("You can only index one level of related object")

            elif hasattr(value, "__iter__"):
                if lookup == lookups[-1]:
                    return value
                else:
                    raise TypeError("You can only index one level of iterable")

        return [ value ]

    for field in fields_to_index:
        texts = get_data_from_field(field, instance)
        for text in texts:
            if text is None:
                continue

            text = smart_unicode(text)
            text = text.lower() #Normalize

            words = text.split(" ") #Split on whitespace

            #Build up combinations of adjacent words
            for i in xrange(0, len(words)):
                for j in xrange(1, 5):
                    term_words = words[i:i+j]

                    if len(term_words) != j:
                        break

                    term = u" ".join(term_words)

                    if not term.strip(): continue

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

def _unindex_then_reindex(instance, fields_to_index):
    unindex_instance(instance)
    _do_index(instance, fields_to_index)

def index_instance(instance, fields_to_index, defer_index=True):
    """
        If we are in a transaction, we must defer the unindex and reindex :(
    """

    if db.is_in_transaction() or defer_index:
        defer(_unindex_then_reindex, instance, fields_to_index, _queue=QUEUE_FOR_INDEXING)
    else:
        unindex_instance(instance)
        _do_index(instance, fields_to_index)

def unindex_instance(instance):
    indexes = Index.objects.filter(instance_db_table=instance._meta.db_table, instance_pk=instance.pk).all()
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
            logging.warning("A GlobalOccuranceCount for Index: %s does not exist, ignoring", index.pk)
            continue



def parse_terms(search_string):
    return shlex.split(smart_str(search_string.lower()))

def search(model_class, search_string, per_page=50, current_page=1, total_pages=10, **filters):
    terms = parse_terms(search_string)

    #Get all matching terms
    matching_terms = dict(GlobalOccuranceCount.objects.filter(pk__in=terms).values_list('pk', 'count'))
    matches = Index.objects.filter(iexact__in=terms, instance_db_table=model_class._meta.db_table).all()

    instance_weights = {}

    for match in matches:
        instance_weights.setdefault(match.instance_pk, []).append(matching_terms[match.iexact])

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

    #Restrict to the max possible
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

class Index(models.Model):
    iexact = models.CharField(max_length=1024)
    instance_db_table = models.CharField(max_length=1024)
    instance_pk = models.PositiveIntegerField(default=0)
    occurances = models.PositiveIntegerField(default=0)

    class Meta:
        unique_together = [
            ('iexact', 'instance_db_table', 'instance_pk')
        ]

from django.dispatch import receiver
from django.db.models.signals import post_save, pre_delete

@receiver(post_save)
def post_save_index(sender, instance, created, raw, *args, **kwargs):
    if getattr(instance, "Search", None):
        fields_to_index = getattr(instance.Search, "fields", [])
        if fields_to_index:
            index_instance(instance, fields_to_index, defer_index=not raw) #Don't defer if we are loading from a fixture

@receiver(pre_delete)
def pre_delete_unindex(sender, instance, using, *args, **kwarg):
    if getattr(instance, "Search", None):
        unindex_instance(instance)
