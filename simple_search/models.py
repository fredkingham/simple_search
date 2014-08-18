from django.db import models

from base_models import AbstractIndex, AbstractIndexRecord


"""
    REMAINING TO DO!

    1. Partial matches. These should be recorded as new Index records, with an FK to the full term being indexed.
       Partials should only be recorded for between 4, and len(original_term) - 1 characters. Partial matches should be much more highly scored,
       the lower the match, the more the score should be
    2. Cross-join indexing  e.g. book__title on an Author.
    3. Field matches. e.g "id:1234 field1:banana". This should match any other words using indexes, but only return matches that match the field lookups
"""


class IndexRecord(AbstractIndexRecord):
    instance_db_table = models.CharField(max_length=1024)
    instance_pk = models.PositiveIntegerField(default=0)

    class Meta:
        unique_together = [
            ('iexact', 'field', 'instance_db_table', 'instance_pk')
        ]

    OBJECT_ID_FIELD = 'instance_pk'

class Index(AbstractIndex):
    indexrecord_class = IndexRecord

    def create_record(self, obj, field, iexact, occurances):
        """ Create an index record from django model instance obj """
        self.indexrecord_class.objects.create(
            iexact=iexact,
            instance_db_table=obj._meta.db_table,
            instance_pk=obj.pk,
            field=field,
            occurances=occurances
        )

    def _get_records(self, instance):
        return self.indexrecord_class.objects.filter(
            instance_db_table=instance._meta.db_table, instance_pk=instance.pk).all()

    def search(self, model_class, search_string, per_page=50, current_page=1, total_pages=10, **filters):
        terms = self.parse_terms(search_string)

        obj_weights = self._get_matches(terms, extra_filters={'instance_db_table': model_class._meta.db_table})
        order = self._get_result_order(obj_weights, per_page, current_page, total_pages)

        instance_pks = [x.instance_pk for x in order.keys()]

        queryset = model_class.objects.all()

        if filters:
            queryset = queryset.filter(**filters)

        results = queryset.filter(pk__in=instance_pks)
        results_by_pk = {x.pk: x for x in results}

        # remove duplicates, maintain the order, exclude items that are excluded by the filters
        seen = set()

        sorted_instances = []

        unordered = ((instance, enumerator,) for instance, enumerator in order.items())

        sorted_order = sorted(unordered, key=lambda x: x[1])
        for result, _ in sorted_order:
            if result.instance_pk not in seen:
                if result.instance_pk in results_by_pk:
                    sorted_instances.append(results_by_pk[result.instance_pk])
                    seen.add(result.instance_pk)

        return sorted_instances


index = Index()

from django.dispatch import receiver
from django.db.models.signals import post_save, pre_delete


@receiver(post_save)
def post_save_index(sender, instance, created, raw, *args, **kwargs):
    if getattr(instance, "Search", None):
        fields_to_index = getattr(instance.Search, "fields", [])
        if fields_to_index:
            index.index(instance, fields_to_index, defer_index=not raw)  # Don't defer if we are loading from a fixture


@receiver(pre_delete)
def pre_delete_unindex(sender, instance, using, *args, **kwarg):
    if getattr(instance, "Search", None):
        index.unindex(instance)
