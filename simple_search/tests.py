"""
This file demonstrates writing tests using the unittest module. These will pass
when you run "manage.py test".

Replace this with more appropriate tests for your application.
"""

import unittest
import mock

try:
    from djangae.fields import ListField
except ImportError:
    from djangotoolbox.fields import ListField

from django.db import models
from django.test import TestCase
#from potatobase.testbase import PotatoTestCase

from .base_models import AbstractIndexRecord, AbstractIndex, GlobalOccuranceCount
from .models import IndexRecord, index


class MockRelatedManager(object):
    def __init__(self, retval):
        self.retval = retval

    def all(self):
        return self.retval


class SampleModel(models.Model):
    field1 = models.CharField(max_length=1024)
    field2 = models.CharField(max_length=1024)
    list_field = ListField(default=[])

    related_field = models.ForeignKey('self', blank=True, null=True)

    def __unicode__(self):
        return u"{} - {}".format(self.field1, self.field2)


class TestIndexRecord(AbstractIndexRecord):
    """ Could just use Index, but this is the most minimal index possible. """
    obj_reference = models.CharField(max_length=255)
    OBJECT_ID_FIELD = 'obj_reference'


class TestIndex(AbstractIndex):
    indexrecord_class = TestIndexRecord
test_index = TestIndex()

class SearchTests(TestCase):
    def test_field_indexing(self):
        instance1 = SampleModel.objects.create(
            field1="bananas apples cherries plums oranges kiwi"
        )
        i = index
        i.index(instance1, ["field1"], defer_index=False)

        self.assertEqual(1, IndexRecord.objects.filter(iexact="banana").count())
        self.assertEqual(1, IndexRecord.objects.filter(iexact="banana appl").count())
        self.assertEqual(1, IndexRecord.objects.filter(iexact="banana appl cherri").count())
        self.assertEqual(1, IndexRecord.objects.filter(iexact="banana appl cherri plum").count())

        #We only store up to 4 adjacent words
        self.assertEqual(0, IndexRecord.objects.filter(iexact="banana apple cherri plum orang").count())

        self.assertEqual(1, IndexRecord.objects.filter(iexact="appl").count())
        self.assertEqual(1, IndexRecord.objects.filter(iexact="appl cherri").count())
        self.assertEqual(1, IndexRecord.objects.filter(iexact="appl cherri plum").count())
        self.assertEqual(1, IndexRecord.objects.filter(iexact="appl cherri plum orang").count())

        #We only store up to 4 adjacent words
        self.assertEqual(0, IndexRecord.objects.filter(iexact="appl cherri plum orange kiwi").count())

    def test_ordering(self):
        instance1 = SampleModel.objects.create(field1="a search term with some unique words banana fish")
        instance2 = SampleModel.objects.create(field1="another search term with a unique word fish")
        instance3 = SampleModel.objects.create(field1="not so unique")

        index.index(instance1, ["field1"], defer_index=False)
        index.index(instance2, ["field1"], defer_index=False)
        index.index(instance3, ["field1"], defer_index=False)

        results = index.search(SampleModel, "search unique words")

        #Instance 3 should come last, because it only contains "a"
        self.assertEqual(instance3, results[2], results)

        results = index.search(SampleModel, "banana fish")

        self.assertEqual(instance1, results[0])  # Instance 1 matches 2 uncommon words
        self.assertEqual(instance2, results[1])  # Instance 2 matches 1 uncommon word

    def test_basic_searching(self):
        self.assertEqual(0, SampleModel.objects.count())
        self.assertEqual(0, GlobalOccuranceCount.objects.count())

        instance1 = SampleModel.objects.create(field1="Banana", field2="Apple")
        instance2 = SampleModel.objects.create(field1="banana", field2="Cherry")
        instance3 = SampleModel.objects.create(field1="BANANA")

        index.index(instance1, ["field1", "field2"], defer_index=False)
        self.assertEqual(2, IndexRecord.objects.count())
        self.assertEqual(1, GlobalOccuranceCount.objects.get(pk="banana").count)
        self.assertEqual(1, GlobalOccuranceCount.objects.get(pk="appl").count)

        index.index(instance2, ["field1", "field2"], defer_index=False)

        self.assertEqual(4, IndexRecord.objects.count())
        self.assertEqual(2, GlobalOccuranceCount.objects.get(pk="banana").count)
        self.assertEqual(1, GlobalOccuranceCount.objects.get(pk="appl").count)
        self.assertEqual(1, GlobalOccuranceCount.objects.get(pk="cherri").count)

        index.index(instance3, ["field1"], defer_index=False)
        self.assertEqual(5, IndexRecord.objects.count())
        self.assertEqual(3, GlobalOccuranceCount.objects.get(pk="banana").count)
        self.assertEqual(1, GlobalOccuranceCount.objects.get(pk="appl").count)
        self.assertEqual(1, GlobalOccuranceCount.objects.get(pk="cherri").count)

        self.assertItemsEqual([instance1, instance2, instance3], index.search(SampleModel, "banana"))
        self.assertItemsEqual([instance2], index.search(SampleModel, "cherry"))

        index.unindex(instance1)

        self.assertItemsEqual([instance2, instance3], index.search(SampleModel, "banana"))
        self.assertItemsEqual([instance2], index.search(SampleModel, "cherry"))

    def test_leading_underscore_search(self):
        instance1 = SampleModel.objects.create(field1="__testing__", field2="Apple")
        index.index(instance1, ["field1", "field2"], defer_index=False)
        self.assertItemsEqual([instance1], index.search(SampleModel, "__testing__"))
        self.assertItemsEqual([instance1], index.search(SampleModel, "testing__"))

    def test_empty_search(self):
        index.search(SampleModel, '""')

    def test_additional_filters(self):
        instance1 = SampleModel.objects.create(field1="banana", field2="apple")
        instance2 = SampleModel.objects.create(field1="banana", field2="cherry")
        instance3 = SampleModel.objects.create(field1="pineapple", field2="apple")

        index.index(instance1, ["field2"], defer_index=False)
        index.index(instance2, ["field2"], defer_index=False)
        index.index(instance3, ["field2"], defer_index=False)

        self.assertItemsEqual([instance1, instance3], index.search(SampleModel, "apple"))

        # Now pass to search a queryset filter and check that it's applied
        self.assertItemsEqual([instance1], index.search(SampleModel, "apple", **{'field1': 'banana'}))

    @unittest.skip("Not implemented yet")
    def test_logic_searching(self):
        instance1 = SampleModel.objects.create(field1="Banana", field2="Apple")
        instance2 = SampleModel.objects.create(field1="banana", field2="Cherry")
        instance3 = SampleModel.objects.create(field1="BANANA")

        index.index(instance1, ["field1", "field2"], defer_index=False)
        index.index(instance2, ["field1", "field2"], defer_index=False)
        index.index(instance3, ["field1"], defer_index=False)

        self.assertItemsEqual([instance1], index.search(SampleModel, "banana AND apple"))
        self.assertItemsEqual([instance1, instance2], index.search(SampleModel, "apple OR cherry"))

        index.unindex(instance1)

        self.assertItemsEqual([], index.search(SampleModel, "banana AND apple"))
        self.assertItemsEqual([instance2], index.search(SampleModel, "apple OR cherry"))



class IndexTests(TestCase):
    def test_get_dict_data(self):
        """ Tests getting data from indexable objects, both plain (dict) ones and django instances. """
        obj = {'somelist':[1,2,3], 'something else': 'horplecrump'}

        self.assertEqual(test_index.get_field_data('somelist', obj), [1, 2, 3])
        self.assertEqual(test_index.get_field_data('something else', obj), ['horplecrump'])

    def test_get_model_data(self):
        """ Tests getting data from indexable objects, both plain (dict) ones and django instances. """
        obj = SampleModel(list_field=[1,2,3], field1='horplecrump')
        obj2 = SampleModel(related_field=obj)

        self.assertEqual(test_index.get_field_data('list_field', obj), [1, 2, 3])
        self.assertEqual(test_index.get_field_data('field1', obj), ['horplecrump'])

        with mock.patch('simple_search.tests.SampleModel.samplemodel_set', new=MockRelatedManager(retval=[obj2])):
            test_index.get_field_data('samplemodel_set__field1', obj)

class UniquenessTests(TestCase):
    def test_index_uniqueness(self):
        """ Test an object can be indexed if it contains non-unique data in different fields.
            This is to make sure unique_together on Index is set up right.
        """
        obj = SampleModel(id=1, list_field=[1,2,3], field1='horplecrump', field2='horplecrump')

        with mock.patch('simple_search.tests.TestIndex._get_records', return_value=[]):
            index.reindex(obj, fields_to_index=['field1', 'field2'])

class ParseTermsTests(TestCase):
    def test_parse_terms(self):
        self.assertEqual(AbstractIndex.parse_terms('"This:isn\'t a field"'), {None:["this:isn't a field"]})
        self.assertEqual(AbstractIndex.parse_terms("test"), {None:["test"]})
        self.assertEqual(AbstractIndex.parse_terms("test field:test1, other_field:test2"), {None:["test"], "field":["test1"], "other_field":["test2"]})
        self.assertEqual(AbstractIndex.parse_terms("test1 test2"), {None:["test1", "test2"]})
        self.assertEqual(AbstractIndex.parse_terms("This: is multiple things"), {None:["multipl", "thing"]})
        self.assertEqual(AbstractIndex.parse_terms("key:value also multiple things"), {"key":["valu"], None:["also", "multipl", "thing"]})

class CanonicalizeTests(TestCase):
    def test_canonicalize(self):
        self.assertEqual(AbstractIndex.canonicalize("a it the development at if"), ["develop"])
        self.assertEqual(AbstractIndex.canonicalize("a it the development at if", remove_stopwords=False), ["a", "it", "the", "develop", "at", "if"])
        self.assertEqual(AbstractIndex.canonicalize("a it the development at if", do_stemming=False), ["development"])
        self.assertEqual(AbstractIndex.canonicalize("how__ do you like __dem__ apples",), ["how__", "like", "dem__", "appl"])
