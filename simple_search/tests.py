"""
This file demonstrates writing tests using the unittest module. These will pass
when you run "manage.py test".

Replace this with more appropriate tests for your application.
"""

import unittest
from django.db import models
from django.test import TestCase
from potatobase.testbase import PotatoTestCase

from .models import Index, GlobalOccuranceCount


class SampleModel(models.Model):
    field1 = models.CharField(max_length=1024)
    field2 = models.CharField(max_length=1024)

    def __unicode__(self):
        return u"{} - {}".format(self.field1, self.field2)


class SearchTests(PotatoTestCase):
    def test_field_indexing(self):
        instance1 = SampleModel.objects.create(
            field1="bananas apples cherries plums oranges kiwi"
        )
        i = Index()
        i.index(instance1, ["field1"], defer_index=False)

        self.assertEqual(1, Index.objects.filter(iexact="bananas").count())
        self.assertEqual(1, Index.objects.filter(iexact="bananas apples").count())
        self.assertEqual(1, Index.objects.filter(iexact="bananas apples cherries").count())
        self.assertEqual(1, Index.objects.filter(iexact="bananas apples cherries plums").count())

        #We only store up to 4 adjacent words
        self.assertEqual(0, Index.objects.filter(iexact="bananas apples cherries plums oranges").count())

        self.assertEqual(1, Index.objects.filter(iexact="apples").count())
        self.assertEqual(1, Index.objects.filter(iexact="apples cherries").count())
        self.assertEqual(1, Index.objects.filter(iexact="apples cherries plums").count())
        self.assertEqual(1, Index.objects.filter(iexact="apples cherries plums oranges").count())

        #We only store up to 4 adjacent words
        self.assertEqual(0, Index.objects.filter(iexact="apples cherries plums oranges kiwis").count())

    def test_ordering(self):
        instance1 = SampleModel.objects.create(field1="eat a fish")
        instance2 = SampleModel.objects.create(field1="eat a chicken")
        instance3 = SampleModel.objects.create(field1="sleep a lot")

        i = Index()
        i.index(instance1, ["field1"], defer_index=False)
        i.index(instance2, ["field1"], defer_index=False)
        i.index(instance3, ["field1"], defer_index=False)

        results = i.search(SampleModel, "eat a")

        #Instance 3 should come last, because it only contains "a"
        self.assertEqual(instance3, results[2], results)

        results = i.search(SampleModel, "eat fish")

        self.assertEqual(instance1, results[0])  # Instance 1 matches 2 uncommon words
        self.assertEqual(instance2, results[1])  # Instance 2 matches 1 uncommon word

    def test_basic_searching(self):
        self.assertEqual(0, SampleModel.objects.count())
        self.assertEqual(0, GlobalOccuranceCount.objects.count())

        instance1 = SampleModel.objects.create(field1="Banana", field2="Apple")
        instance2 = SampleModel.objects.create(field1="banana", field2="Cherry")
        instance3 = SampleModel.objects.create(field1="BANANA")

        i = Index()

        i.index(instance1, ["field1", "field2"], defer_index=False)
        self.assertEqual(2, Index.objects.count())
        self.assertEqual(1, GlobalOccuranceCount.objects.get(pk="banana").count)
        self.assertEqual(1, GlobalOccuranceCount.objects.get(pk="apple").count)

        i.index(instance2, ["field1", "field2"], defer_index=False)

        self.assertEqual(4, Index.objects.count())
        self.assertEqual(2, GlobalOccuranceCount.objects.get(pk="banana").count)
        self.assertEqual(1, GlobalOccuranceCount.objects.get(pk="apple").count)
        self.assertEqual(1, GlobalOccuranceCount.objects.get(pk="cherry").count)

        i.index(instance3, ["field1"], defer_index=False)
        self.assertEqual(5, Index.objects.count())
        self.assertEqual(3, GlobalOccuranceCount.objects.get(pk="banana").count)
        self.assertEqual(1, GlobalOccuranceCount.objects.get(pk="apple").count)
        self.assertEqual(1, GlobalOccuranceCount.objects.get(pk="cherry").count)

        self.assertItemsEqual([instance1, instance2, instance3], i.search(SampleModel, "banana"))
        self.assertItemsEqual([instance2], i.search(SampleModel, "cherry"))

        i.unindex(instance1)

        self.assertItemsEqual([instance2, instance3], i.search(SampleModel, "banana"))
        self.assertItemsEqual([instance2], i.search(SampleModel, "cherry"))

    def test_additional_filters(self):
        instance1 = SampleModel.objects.create(field1="banana", field2="apple")
        instance2 = SampleModel.objects.create(field1="banana", field2="cherry")
        instance3 = SampleModel.objects.create(field1="pineapple", field2="apple")

        i = Index()
        i.index(instance1, ["field2"], defer_index=False)
        i.index(instance2, ["field2"], defer_index=False)
        i.index(instance3, ["field2"], defer_index=False)

        self.assertItemsEqual([instance1, instance3], i.search(SampleModel, "apple"))

        # Now pass to search a queryset filter and check that it's applied
        self.assertItemsEqual([instance1], i.search(SampleModel, "apple", **{'field1': 'banana'}))

    @unittest.skip("Not implemented yet")
    def test_logic_searching(self):
        instance1 = SampleModel.objects.create(field1="Banana", field2="Apple")
        instance2 = SampleModel.objects.create(field1="banana", field2="Cherry")
        instance3 = SampleModel.objects.create(field1="BANANA")

        i = Index()
        i.index(instance1, ["field1", "field2"], defer_index=False)
        i.index(instance2, ["field1", "field2"], defer_index=False)
        i.index(instance3, ["field1"], defer_index=False)

        self.assertItemsEqual([instance1], i.search(SampleModel, "banana AND apple"))
        self.assertItemsEqual([instance1, instance2], i.search(SampleModel, "apple OR cherry"))

        i.unindex(instance1)

        self.assertItemsEqual([], i.search(SampleModel, "banana AND apple"))
        self.assertItemsEqual([instance2], i.search(SampleModel, "apple OR cherry"))
