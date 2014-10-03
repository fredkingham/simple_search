# -*- encoding: utf-8 -*-

import logging
import re
import time

import nltk
from django.db import models
from django.utils.encoding import smart_unicode
from google.appengine.ext import db
from google.appengine.ext.deferred import defer
from django.conf import settings

QUEUE_FOR_INDEXING = getattr(settings, "QUEUE_FOR_INDEXING", "default")


class GlobalOccuranceCount(models.Model):
    id = models.CharField(max_length=1024, primary_key=True)
    count = models.PositiveIntegerField(default=0)

    def update(self, index_class):
        count = sum(index_class.objects.filter(iexact=self.id).values_list('occurances', flat=True))

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


class AbstractIndexRecord(models.Model):
    iexact = models.CharField(max_length=1024)
    occurances = models.PositiveIntegerField(default=0)
    field = models.CharField(max_length=100) # Field on indexed obj containing iexact

    class Meta:
        abstract = True

    def __init__(self, *args, **kwargs):
        if not self.__class__.OBJECT_ID_FIELD:
            raise Exception("Simple_search misconfigured, no OBJECT_ID_FIELD set in class %s" % self.__class__)
        super(AbstractIndexRecord, self).__init__(*args, **kwargs)

    # Override the following methods and set following attributes in inheriting classes

    # Field in the index used to identify the indexed object. This could be the primary key of a django object,
    # or any kind of resource identifier.
    OBJECT_ID_FIELD = ''

    def delete(self):
        """ Remove a single index record. """

        @db.transactional(xg=True)
        def txn(record):
            count = GlobalOccuranceCount.objects.get(pk=record.iexact)
            if count.count <= record.occurances:
                count.delete()
            else:
                count.count -= record.occurances
                count.save()
            super(AbstractIndexRecord, record).delete()

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


class AbstractIndex(object):
    indexrecord_class = None

    def __init__(self):
        if not getattr(self, 'indexrecord_class', None):
            raise Exception("Misconfigured %s: indexrecord_class needs to be set." % self.__class__)

    def _get_records(self, obj):
        """ Get all index records that belong to an object. """
        raise NotImplementedError("Subclasses should implement this.")

    def get_or_create_record(self, obj, field, iexact, occurances):
        """ Simple wrapper around get_or_create to all different index classes to specify how to create the record.
            Returns a tuple of (record, created), just like get_or_create
        """
        raise NotImplementedError("Subclasses should implement this.")

    def search(self, *args, **kwargs):
        """ Perform a search on the index. """
        raise NotImplementedError("Subclasses should implement this.")

    # End of unimplemented methods.

    def index(self, obj, fields_to_index, defer_index=True):
        """ Index an object. Will defer the indexing if defer_index is true or if called inside a transaction.
            Indexing an object will always unindex the object first.
        """
        if db.is_in_transaction() or defer_index:
            defer(self.reindex, obj, fields_to_index, defer_index=defer_index, _queue=QUEUE_FOR_INDEXING)
        else:
            self.reindex(obj, fields_to_index, defer_index=defer_index)

    def reindex(self, obj, fields_to_index, defer_index=True):
        """ Unindex the object, then call _do_index to do the actual indexing work. """
        self.unindex(obj)
        self._do_index(obj, fields_to_index, defer_index=defer_index)

    def unindex(self, obj):
        """ Unindex an object by deleting all records referencing it. """

        records = self._get_records(obj)
        for record in list(records):
            try:
                record.delete()
            except AssertionError:
                logging.exception("Something went wrong while unindexing an index record.")

    def _generate_terms(self, text):
        """ Takes a string, splits it into words and generates a list of combinations of adjacent words.
            The terms are limited to 4 words in length.

            Example:
            Input: "Yo, what's up?"
            Output: ["yo,", "yo, what's", "yo, what's up?", "what's", "what's up?", "up?"]
        """
        if text is None:
            return []

        stems = self.canonicalize(text)

        terms = []
        #Build up combinations of adjacent words
        for i in xrange(0, len(stems)):
            for j in xrange(1, 5):
                term_words = stems[i:i+j]

                if len(term_words) != j:
                    break

                term = u" ".join(term_words)

                if not term.strip():
                    continue
                terms.append(term)
        return terms

    def _index_term(self, obj, field, text, term):
        # FIXME: I've had to disable this transaction because get_or_create doesn't work inside transactions
        # It also doesn't (reliably) work outside transactions. This can be reenabled once djangae has unique-caching.
        #@db.transactional(xg=True)
        text = ' '.join(self.canonicalize(text))
        def txn(term_):
            #logging.info("Indexing: '%s', %s", term_, type(term_))
            term_count = text.count(term_)
            self.get_or_create_record(obj, field, term_, term_count)

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

    def _do_index(self, obj, fields_to_index, defer_index=True):
        """ Index an object. Fields_to_index can refer to instance attributes or dictionary keys,
            self.get_field_data is used to get the actual data, which can be overwritten for specific requirements.
        """
        logging.info("[SIMPLE_SEARCH] Indexing object %s, spawning _index_term tasks" % obj)
        for field in fields_to_index:
            texts = self.get_field_data(field, obj)

            for text in texts:
                terms = self._generate_terms(text)
                for term in terms:
                    if defer_index:
                        defer(self._index_term, obj, field, text, term, _queue=settings.QUEUE_FOR_INDEXING)
                    else:
                        self._index_term(obj, field, text, term)

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
        for record, matching_terms in obj_weights.items():

            n = float(len(matching_terms))
            final_weights.append((sum(matching_terms) / (n + ((n-1) * 0.5)), record))

        final_weights.sort(key=lambda x: x[0])

        return final_weights

    def _get_result_order(self, obj_weights, per_page, current_page, total_pages):
        """ Generate an order for object weights, taking into account any paging necessary. """

        final_weights = []
        final_weights = self._weight_results(obj_weights)
        final_weights = self._apply_paging_to_results(final_weights, per_page, current_page, total_pages)

        # just return the match objects
        return [x[1] for x in final_weights]

    def _get_matches(self, terms, extra_filters=None):
        matching_terms = dict(list(GlobalOccuranceCount.objects.filter(pk__in=terms).values_list('pk', 'count')))

        filter_args = {'iexact__in': terms}
        if extra_filters:
            filter_args.update(extra_filters)

        matches = self.indexrecord_class.objects.filter(**filter_args).all()

        obj_weights = {}

        for match in matches:
            try:
                obj_weights.setdefault(match, []).append(matching_terms[match.iexact])
            except:
                logging.critical("[_get_matches] %s wasn't found in matching_terms. Not adding this match to obj_weights" % match.iexact)

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
        if type(data) in (list, tuple):
            return data
        return [obj[field]]

    def get_field_data(self, field, obj):
        """ Gets indexable data from an object.

            If obj is a django model instance, this will get attributes from the object,
            as well as from related instances using related__field syntax.
            Only allows for one level of related objects and iterables.

            if the object is a dictionary, it will simply return [obj[field]].

            To customise this behaviour, override _get_*_data magic methods as necessary, which should always returns lists of values
        """

        obj_classname = obj.__class__.__name__.lower()

        get_data_method = getattr(self.__class__, "_get_%s_data" % obj_classname, None)
        if get_data_method:
            return get_data_method(self, field, obj)

        # If no specific method was defined, but the object is a model instance, use the generic _get_model_data
        if isinstance(obj, models.Model):
            return self._get_model_data(field, obj)

        raise Exception("Object type %s is not supported by index. Add a get_<type.lower()>_data function to support it.", obj_classname)

    @classmethod
    def canonicalize(cls, raw, remove_stopwords=True, do_stemming=True):
        """ :param remove_stopwords: Remove words like 'the', 'a' 'an' etc.
            :param do_stemming: Return stem version of word, i.e. [walk walking walked] -> walk
        """
        if remove_stopwords:
            stopwords = nltk.corpus.stopwords.words('english')  # todo support other languages

        if do_stemming:
            stemmer = nltk.stem.porter.PorterStemmer()

        normalized = cls.normalize(raw)
        tokenized = nltk.word_tokenize(normalized)

        tokens = []
        for token in tokenized:
            if remove_stopwords and token in stopwords:
                continue
            if do_stemming:
                token = stemmer.stem(token)
                if not token.strip(":\""):  # remove any renmants of fields
                    continue
            if token.startswith("__"):
                # Remove leading underscores. GlobalOccuranceCounts use the token as a primary key,
                # and the datastore doesn't allow pks that start with underscores.
                token = re.sub("^_+", "", token)

            tokens.append(token.lower())

        return tokens

    @staticmethod
    def normalize(s):
        whitespace_characters = u'|/-–—~,.;:!?'
        for char in whitespace_characters:
            s = s.replace(char, ' ')
        # Replace some characters with whitespace
        return smart_unicode(s).lower()

    @classmethod
    def parse_terms(cls, search_string):
        """ For a string containing several search terms, which can have be labeled and/or grouped with quotes,
            this returns a dict of {label:[search_tokens]}

            Examples:
            "This:isn't a field" -> {None: ["This:isn't a field"]}
            This:"is a field" -> {"This": ["is a field"]}
            This:is multiple things -> {"This": ["is"], None: ["multiple", "things"]}
        """
        search_string = cls.normalize(search_string)

        split_by_space = r'(?:[^\s,"]|"(?:\\.|[^"])*")+'
        split_field = r'^(?P<field>[^:"]+):[^ ]+'

        def get_field_content(token, field):
            if field:
                token = re.sub(field+":", '', token)

            if token.startswith('"') and token.endswith('"'):
                token = token[1:-1]

            return field, token

        # Separate search text into two lists with one entry per search term:
        #   fields: field name to be searched(or None)
        #   search_strings: actual string to search for
        tokens = [s for s in re.findall(split_by_space, search_string)]
        fields = [match[0] if len(match) == 1 else None for match in [re.findall(split_field, token) for token in tokens]]
        field_contents = map(get_field_content, tokens, fields)

        raw_parsed_terms = {}
        for field, token in field_contents:
            raw_parsed_terms.setdefault(field, []).append(token)


        parsed_terms = {field: [] for field in raw_parsed_terms}
        for field, tokens in raw_parsed_terms.iteritems():
            unquoted = []
            for token in tokens:
                # # Remove empty values
                # if not token:
                #     continue

                if re.search(r'\s', token):
                    parsed_terms[field].append(" ".join(cls.canonicalize(token)))
                else:
                    unquoted.append(token)

            canon_terms = cls.canonicalize(' '.join(unquoted))

            parsed_terms[field].extend(canon_terms)
        return parsed_terms
