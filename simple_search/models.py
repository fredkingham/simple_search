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

    1. Partial matches. These should be recorded as new Index records, with an FK to the full term being indexed.
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

    def __init__(self, *args, **kwargs):
        if not self.__class__.OBJECT_ID_FIELD:
            raise Exception("Simple_search misconfigured, no OBJECT_ID_FIELD set in class %s" % self.__class__)
        super(AbstractIndex, self).__init__(*args, **kwargs)

    # Override the following methods and set following attributes in inheriting classes

    # Field in the index used to identify the indexed object. This could be the primary key of a django object,
    # or any kind of resource identifier.
    OBJECT_ID_FIELD = ''

    def _get_records(self, obj):
        """ Get all index records that belong to an object. """
        raise NotImplementedError("Subclasses should implement this.")

    def create_record(self, obj, iexact, occurances):
        """ Create a record from an object, its iexact text and the number of occurances. """
        NotImplementedError("Subclasses should implement this.")

    def search(self, *args, **kwargs):
        """ Perform a search on the index. """
        raise NotImplementedError("Subclasses should implement this.")

    # End of unimplemented methods.

    def index(self, obj, fields_to_index, defer_index=True):
        """ Index an object. Will defer the indexing if defer_index is true or if called inside a transaction.
            Indexing an object will always unindex the object first.
        """
        if db.is_in_transaction() or defer_index:
            defer(self.reindex, obj, fields_to_index, _queue=QUEUE_FOR_INDEXING)
        else:
            self.reindex(obj, fields_to_index)

    def reindex(self, obj, fields_to_index):
        """ Unindex the object, then call _do_index to do the actual indexing work. """
        self.unindex(obj)
        self._do_index(obj, fields_to_index)

    def unindex(self, obj):
        """ Unindex an object by deleting all records referencing it. """

        records = self._get_records(obj)
        for record in records:
            record.delete()

    def delete(self):
        """ Remove a single index record. """

        @db.transactional(xg=True)
        def txn(record):
            count = GlobalOccuranceCount.objects.get(pk=record.iexact)
            count.count -= record.occurances
            count.save()
            super(AbstractIndex, record).delete()

        try:
            while True:
                try:
                    txn(self)
                    break
                except db.TransactionFailedError:
                    logging.warning("Transaction collision, retrying!")
                    time.sleep(1)
                    continue
        except GlobalOccuranceCount.DoesNotExist:
            logging.warning(
                "A GlobalOccuranceCount for Index: %s "
                "does not exist, ignoring", self.pk
            )

    def _generate_terms(self, text):
        """ Takes a string, splits it into words and generates a list of combinations of adjacent words.
            The terms are limited to 4 words in length.

            Example:
            Input: "Yo, what's up?"
            Output: ["yo,", "yo, what's", "yo, what's up?", "what's", "what's up?", "up?"]
        """
        if text is None:
            return []

        text = self.normalize(text)

        words = text.split(" ")  # Split on whitespace

        terms = []
        #Build up combinations of adjacent words
        for i in xrange(0, len(words)):
            for j in xrange(1, 5):
                term_words = words[i:i+j]

                if len(term_words) != j:
                    break

                term = u" ".join(term_words)

                if not term.strip():
                    continue
                terms.append(term)
        return terms

    def _do_index(self, obj, fields_to_index):
        """ Index an object. Fields_to_index can refer to instance attributes or dictionary keys,
            self.get_field_data is used to get the actual data, which can be overwritten for specific requirements.
        """
        for field in fields_to_index:
            texts = self.get_field_data(field, obj)
            for text in texts:
                terms = self._generate_terms(text)
                for term in terms:
                    @db.transactional(xg=True)
                    def txn(term_):
                        logging.info("Indexing: '%s', %s", term_, type(term_))

                        term_count = self.normalize(text).count(term_)
                        self.create_record(obj, term_, term_count)

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

    def _weight_results(self, obj_weights):
        """
            This is where we rank the results. Lower scores are better. Scores are based
            on the commonality of the word. More matches are rewarded, but not too much so
            that rarer terms still have a chance.

            Examples for n matches:

            1 = 1 + (0 * 0.5) = 1    -> scores / 1
            2 = 2 + (1 * 0.5) = 2.5  -> scores / 2.5 (rather than 2)
            3 = 3 + (2 * 0.5) = 4    -> scores / 4 (rather than 3)
        """
        final_weights = []
        for k, v in obj_weights.items():

            n = float(len(v))
            final_weights.append((sum(v) / (n + ((n-1) * 0.5)), k))

        final_weights.sort()
        return final_weights

    def _get_result_order(self, obj_weights, per_page, current_page, total_pages):
        """ Generate an order for object weights, taking into account any paging necessary. """

        final_weights = []
        final_weights = self._weight_results(obj_weights)
        final_weights = self._apply_paging_to_results(final_weights, per_page, current_page, total_pages)

        order = {}
        for index, (score, pk) in enumerate(final_weights):
            order[pk] = index
        return order

    def _get_matches(self, terms, extra_filters=None):
        """ Get matching terms from the global occurance counts. """
        matching_terms = dict(list(GlobalOccuranceCount.objects.filter(pk__in=terms).values_list('pk', 'count')))

        filter_args = {'iexact__in':terms}
        if extra_filters:
            filter_args.update(extra_filters)

        matches = self.__class__.objects.filter(**filter_args).all()

        obj_weights = {}

        for match in matches:
            obj_identifier = getattr(match, match.OBJECT_ID_FIELD)
            obj_weights.setdefault(obj_identifier, []).append(matching_terms[match.iexact])

        return obj_weights

    def _apply_paging_to_results(self, final_weights, per_page, current_page, total_pages):
        #Restrict to the max possible
        final_weights = final_weights[:total_pages*per_page]

        #Restrict to the page
        offset = ((current_page - 1) * per_page)
        return final_weights[offset:offset + per_page]

    def _get_model_data(self, field, obj):
        lookups = field.split("__")
        value = obj

        for lookup in lookups:
            if value is None:
                continue
            value = getattr(value, lookup)

            if "RelatedManager" in value.__class__.__name__:
                if lookup == lookups[-2]:
                    return [getattr(x, lookups[-1]) for x in value.all()]
                else:
                    raise TypeError("You can only index one level of related object")

            elif hasattr(value, "__iter__") and not isinstance(value, basestring):
                if lookup == lookups[-1]:
                    return value
                else:
                    raise TypeError("You can only index one level of iterable")
        return [value]

    def _get_dict_data(self, field, obj):
        data = obj[field]
        if isinstance(data, list) or isinstance(data, tuple):
            return data
        return [obj[field]]

    def get_field_data(self, field, obj):
        """ Gets indexable data from an object.

            If obj is a django model instance, this will get attributes from the object,
            as well as from related instances using related__field syntax.
            Only allows for one level of related objects and iterables.

            if the object is a dictionary, it will simply return [obj[field]].

            To customise this behaviour, override _get_*_data functions as necessary, which should always returns lists of values
        """

        if isinstance(obj, models.Model):
            return self._get_model_data(field, obj)
        elif isinstance(obj, dict):
            return self._get_dict_data(field, obj)

        raise Exception("Object type %s is not supported by index. Add a get_<type>_data function to support it.", type(obj))

    @staticmethod
    def normalize(s):
        return smart_unicode(s).lower()

    @classmethod
    def parse_terms(cls, search_string):
        return shlex.split(cls.normalize(search_string))


class Index(AbstractIndex):
    instance_db_table = models.CharField(max_length=1024)
    instance_pk = models.PositiveIntegerField(default=0)

    class Meta:
        unique_together = [
            ('iexact', 'instance_db_table', 'instance_pk')
        ]

    OBJECT_ID_FIELD = 'instance_pk'

    def create_record(self, obj, iexact, occurances):
        """ Create an index record from django model instance obj """
        Index.objects.create(
            iexact=iexact,
            instance_db_table=obj._meta.db_table,
            instance_pk=obj.pk,
            occurances=occurances
        )

    def _get_records(self, instance):
        return Index.objects.filter(
            instance_db_table=instance._meta.db_table, instance_pk=instance.pk).all()

    def search(self, model_class, search_string, per_page=50, current_page=1, total_pages=10, **filters):
        terms = self.parse_terms(search_string)

        obj_weights = self._get_matches(terms, extra_filters={'instance_db_table':model_class._meta.db_table})
        order = self._get_result_order(obj_weights, per_page, current_page, total_pages)

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

from django.dispatch import receiver
from django.db.models.signals import post_save, pre_delete


@receiver(post_save)
def post_save_index(sender, instance, created, raw, *args, **kwargs):
    if getattr(instance, "Search", None):
        fields_to_index = getattr(instance.Search, "fields", [])
        if fields_to_index:
            Index.index(instance, fields_to_index, defer_index=not raw)  # Don't defer if we are loading from a fixture


@receiver(pre_delete)
def pre_delete_unindex(sender, instance, using, *args, **kwarg):
    if getattr(instance, "Search", None):
        Index.unindex(instance)
