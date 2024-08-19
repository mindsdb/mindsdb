"""
This file bundles language identification functions.

Modifications (fork): Copyright (c) 2021, Adrien Barbaresi.

Original code: Copyright (c) 2011 Marco Lui <saffsd@gmail.com>.
Based on research by Marco Lui and Tim Baldwin.

See LICENSE file for more info.
"""

import bz2
import json
import logging
import lzma
import pickle

from base64 import b64decode
from collections import Counter
from pathlib import Path
from urllib.parse import parse_qs

import numpy as np


LOGGER = logging.getLogger(__name__)

# model defaults
IDENTIFIER = None
MODEL_FILE = 'data/model.plzma'
NORM_PROBS = False  # Normalize output probabilities.
# NORM_PROBS defaults to False for a small speed increase. It does not
# affect the relative ordering of the predicted classes. It can be
# re-enabled at runtime - see the readme.


def load_model(path=None):
    """
    Convenience method to set the global identifier using a model at a
    specified path.

    @param path to model
    """
    LOGGER.debug('initializing identifier')
    global IDENTIFIER
    if path is None:
        IDENTIFIER = LanguageIdentifier.from_pickled_model(MODEL_FILE)
    else:
        IDENTIFIER = LanguageIdentifier.from_modelpath(path)


def set_languages(langs=None):
    """
    Set the language set used by the global identifier.

    @param langs a list of language codes
    """
    if IDENTIFIER is None:
        load_model()
    return IDENTIFIER.set_languages(langs)


def classify(instance, datatype='uint16'):
    """
    Convenience method using a global identifier instance with the default
    model included in langid.py. Identifies the language that a string is
    written in.

    @param instance a text string. Unicode strings will automatically be utf8-encoded
    @returns a tuple of the most likely language and the confidence score
    """
    if IDENTIFIER is None:
        load_model()
    return IDENTIFIER.classify(instance, datatype=datatype)


def rank(instance):
    """
    Convenience method using a global identifier instance with the default
    model included in langid.py. Ranks all the languages in the model according
    to the likelihood that the string is written in each language.

    @param instance a text string. Unicode strings will automatically be utf8-encoded
    @returns a list of tuples language and the confidence score, in descending order
    """
    if IDENTIFIER is None:
        load_model()
    return IDENTIFIER.rank(instance)


def cl_path(path):
    """
    Convenience method using a global identifier instance with the default
    model included in langid.py. Identifies the language that the file at `path` is
    written in.

    @param path path to file
    @returns a tuple of the most likely language and the confidence score
    """
    if IDENTIFIER is None:
        load_model()
    return IDENTIFIER.cl_path(path)


def rank_path(path):
    """
    Convenience method using a global identifier instance with the default
    model included in langid.py. Ranks all the languages in the model according
    to the likelihood that the file at `path` is written in each language.

    @param path path to file
    @returns a list of tuples language and the confidence score, in descending order
    """
    if IDENTIFIER is None:
        load_model()
    return IDENTIFIER.rank_path(path)


class LanguageIdentifier:
    """
    This class implements the actual language identifier.
    """
    __slots__ = ['nb_ptc', 'nb_pc', 'nb_numfeats', 'nb_classes', 'tk_nextmove', 'tk_output',
                 'norm_probs', '__full_model']

    # new version: speed-up
    @classmethod
    def from_pickled_model(cls, pickled_file, *args, **kwargs):
        # load data
        filepath = str(Path(__file__).parent / pickled_file)
        with lzma.open(filepath) as filehandle:
            nb_ptc, nb_pc, nb_classes, tk_nextmove, tk_output = pickle.load(filehandle)
        nb_numfeats = len(nb_ptc) // len(nb_pc)

        # reconstruct pc and ptc
        nb_pc = np.array(nb_pc)
        nb_ptc = np.array(nb_ptc).reshape(nb_numfeats, len(nb_pc))

        return cls(nb_ptc, nb_pc, nb_numfeats, nb_classes, tk_nextmove, tk_output, *args, **kwargs)

    # legacy methods
    @classmethod
    def from_modelstring(cls, string, *args, **kwargs):
        # load data
        nb_ptc, nb_pc, nb_classes, tk_nextmove, tk_output = pickle.loads(bz2.decompress(b64decode(string)))
        nb_numfeats = len(nb_ptc) // len(nb_pc)

        # reconstruct pc and ptc
        nb_pc = np.array(nb_pc)
        nb_ptc = np.array(nb_ptc).reshape(nb_numfeats, len(nb_pc))

        return cls(nb_ptc, nb_pc, nb_numfeats, nb_classes, tk_nextmove, tk_output, *args, **kwargs)

    @classmethod
    def from_modelpath(cls, path, *args, **kwargs):
        with open(path, 'rb') as f:
            return cls.from_modelstring(f.read(), *args, **kwargs)

    def __init__(self, nb_ptc, nb_pc, nb_numfeats, nb_classes, tk_nextmove, tk_output,
                 norm_probs=NORM_PROBS):
        self.nb_ptc = nb_ptc
        self.nb_pc = nb_pc
        self.nb_numfeats = nb_numfeats
        self.nb_classes = nb_classes
        self.tk_nextmove = tk_nextmove
        self.tk_output = tk_output

        def apply_norm_probs(pd):
            """
            Renormalize log-probs into a proper distribution (sum 1)
            The technique for dealing with underflow is described in
            http://jblevins.org/log/log-sum-exp
            """
            if norm_probs:
                # Ignore overflow when computing the exponential. Large values
                # in the exp produce a result of inf, which does not affect
                # the correctness of the calculation (as 1/x->0 as x->inf).
                # On Linux this does not actually trigger a warning, but on
                # Windows this causes a RuntimeWarning, so we explicitly
                # suppress it.
                with np.errstate(over='ignore'):
                    # legacy formula, there are possibly better alternatives
                    pd = 1/np.exp(pd[None,:] - pd[:,None]).sum(1)
            return pd

        self.norm_probs = apply_norm_probs

        # Maintain a reference to the full model, in case we change our language set
        # multiple times.
        self.__full_model = nb_ptc, nb_pc, nb_classes

    def set_languages(self, langs=None):
        LOGGER.debug("restricting languages to: %s", langs)

        # Unpack the full original model. This is needed in case the language set
        # has been previously trimmed, and the new set is not a subset of the current
        # set.
        nb_ptc, nb_pc, nb_classes = self.__full_model

        if langs is None:
            self.nb_classes = nb_classes
            self.nb_ptc = nb_ptc
            self.nb_pc = nb_pc

        else:
            # We were passed a restricted set of languages. Trim the arrays accordingly
            # to speed up processing.
            for lang in langs:
                if lang not in nb_classes:
                    raise ValueError(f"Unknown language code {lang}")

            subset_mask = np.fromiter((l in langs for l in nb_classes), dtype=bool)
            self.nb_classes = [c for c in nb_classes if c in langs]
            self.nb_ptc = nb_ptc[:, subset_mask]
            self.nb_pc = nb_pc[subset_mask]

    def instance2fv(self, text, datatype='uint16'):
        """
        Map an instance into the feature space of the trained model.

        @param datatype NumPy data type (originally uint32)
        """
        # convert to binary if it isn't already the case
        if isinstance(text, str):
            # fix for surrogates on Windows/NT platforms
            text = text.encode('utf8', errors='surrogatepass')

        # Convert the text to a sequence of ascii values and
        # Count the number of times we enter each state
        state = 0
        indexes = []
        for letter in list(text):
            state = self.tk_nextmove[(state << 8) + letter]
            indexes.extend(self.tk_output.get(state, []))

        # datatype: consider that less feature counts are going to be needed
        arr = np.zeros(self.nb_numfeats, dtype=datatype)
        # Update all the productions corresponding to the state
        for index, value in Counter(indexes).items():
            arr[index] = value

        return arr

    def nb_classprobs(self, fv):
        # compute the partial log-probability of the document given each class
        pdc = np.dot(fv, self.nb_ptc)  # fv @ self.nb_ptc
        # compute the partial log-probability of the document in each class
        return pdc + self.nb_pc

    def classify(self, text, datatype='uint16'):
        """
        Classify an instance.
        """
        fv = self.instance2fv(text, datatype=datatype)
        probs = self.norm_probs(self.nb_classprobs(fv))
        cl = np.argmax(probs)
        return self.nb_classes[cl], probs[cl]

    def rank(self, text):
        """
        Return a list of languages in order of likelihood.
        """
        fv = self.instance2fv(text)
        probs = self.norm_probs(self.nb_classprobs(fv))
        return [(str(k), float(v)) for (v, k) in sorted(zip(probs, self.nb_classes), reverse=True)]

    def cl_path(self, path):
        """
        Classify a file at a given path
        """
        with open(path, 'rb') as f:
            retval = self.classify(f.read())
        return path, retval

    def rank_path(self, path):
        """
        Class ranking for a file at a given path
        """
        with open(path, 'rb') as f:
            retval = self.rank(f.read())
        return path, retval


def application(environ, start_response):
    """
    WSGI-compatible langid web service.
    """
    from wsgiref.util import shift_path_info
    try:
        path = shift_path_info(environ)
    except IndexError:
        # Catch shift_path_info's failure to handle empty paths properly
        path = ''

    if path in {'detect', 'rank'}:
        data = None

        # Extract the data component from different access methods
        if environ['REQUEST_METHOD'] == 'PUT':
            data = environ['wsgi.input'].read(int(environ['CONTENT_LENGTH']))
        elif environ['REQUEST_METHOD'] == 'GET':
            try:
                data = parse_qs(environ['QUERY_STRING'])['q'][0]
            except KeyError:
                # No query, provide a null response.
                status = '200 OK'  # HTTP Status
                response = {
                  'responseData': None,
                  'responseStatus': 200,
                  'responseDetails': None,
                }
        elif environ['REQUEST_METHOD'] == 'POST':
            input_string = environ['wsgi.input'].read(int(environ['CONTENT_LENGTH']))
            try:
                data = parse_qs(input_string)['q'][0]
            except KeyError:
                # No key 'q', process the whole input instead
                data = input_string
        else:
            # Unsupported method
            status = '405 Method Not Allowed'  # HTTP Status
            response = {
                'responseData': None,
                'responseStatus': 405,
                'responseDetails': f"{environ['REQUEST_METHOD']} not allowed",
            }

        if data is not None:
            if path == 'detect':
                pred, conf = classify(data)
                response_data = {'language': pred, 'confidence': conf}
            elif path == 'rank':
                response_data = rank(data)

            status = '200 OK'  # HTTP Status
            response = {
              'responseData': response_data,
              'responseStatus': 200,
              'responseDetails': None,
            }

    else:
        # Incorrect URL
        status = '404 Not Found'
        response = {'responseData': None, 'responseStatus': 404, 'responseDetails': 'Not found'}

    headers = [('Content-type', 'text/javascript; charset=utf-8')]  # HTTP Headers
    start_response(status, headers)
    return [json.dumps(response)]


def main():

    # lazy imports
    import argparse
    import sys

    # parse arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('-s', '--serve', action='store_true', default=False, dest='serve', help='launch web service')
    parser.add_argument('--host', default=None, dest='host', help='host/ip to bind to')
    parser.add_argument('--port', default=9008, dest='port', help='port to listen on')
    parser.add_argument('-v', action='count', dest='verbosity', help='increase verbosity (repeat for greater effect)')
    parser.add_argument('-m', dest='model', help='load model from file')
    parser.add_argument('-l', '--langs', dest='langs', help='comma-separated set of target ISO639 language codes (e.g en,de)')
    parser.add_argument('-r', '--remote', action="store_true", default=False, help='auto-detect IP address for remote access')
    parser.add_argument('-b', '--batch', action="store_true", default=False, help='specify a list of files on the command line')
    parser.add_argument('-d', '--dist', action='store_true', default=False, help='show full distribution over languages')
    parser.add_argument('-u', '--url', help='langid of URL')
    parser.add_argument('--line', action="store_true", default=False, help='process pipes line-by-line rather than as a document')
    parser.add_argument('-n', '--normalize', action='store_true', default=False, help='normalize confidence scores to probability values')
    options = parser.parse_args()

    if options.verbosity:
        logging.basicConfig(level=max((5-options.verbosity)*10, 0))
    else:
        logging.basicConfig()

    if options.batch and options.serve:
        parser.error("cannot specify both batch and serve at the same time")

    # unpack a model
    global IDENTIFIER

    if options.model:
        try:
            IDENTIFIER = LanguageIdentifier.from_modelpath(options.model, norm_probs=options.normalize)
            LOGGER.info("Using external model: %s", options.model)
        except IOError as e:
            LOGGER.warning("Failed to load %s: %s", options.model, e)

    if IDENTIFIER is None:
        IDENTIFIER = LanguageIdentifier.from_pickled_model(MODEL_FILE, norm_probs=options.normalize)
        LOGGER.info("Using internal model")

    if options.langs:
        langs = options.langs.split(",")
        IDENTIFIER.set_languages(langs)

    def _process(text):
        """
        Set up a local function to do output, configured according to our settings.
        """
        return IDENTIFIER.rank(text) if options.dist else IDENTIFIER.classify(text)

    if options.url:
        from urllib.request import urlopen
        with urlopen(options.url) as url:
            text = url.read()
            output = _process(text)
            print(options.url, len(text), output)

    elif options.serve:
        import socket
        from wsgiref.simple_server import make_server

        # from http://stackoverflow.com/questions/166506/finding-local-ip-addresses-in-python
        if options.remote and options.host is None:
            # resolve the external ip address
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.connect(("google.com", 80))
            hostname = s.getsockname()[0]
        elif options.host is None:
            # resolve the local hostname
            hostname = socket.gethostbyname(socket.gethostname())
        else:
            hostname = options.host

        print("Listening on %s:%d" % (hostname, int(options.port)))
        print("Press Ctrl+C to exit")
        httpd = make_server(hostname, int(options.port), application)
        try:
            httpd.serve_forever()
        except KeyboardInterrupt:
            pass

    elif options.batch:
        # Start in batch mode - interpret input as paths rather than content
        # to classify.
        import csv
        from multiprocessing import Pool

        def generate_paths():
            for line in sys.stdin:
                path = line.strip()
                if path and Path.is_file(path):
                    yield path

        writer = csv.writer(sys.stdout)
        with Pool() as pool:
            if options.dist:
                writer.writerow(['path'] + IDENTIFIER.nb_classes)
                for path, ranking in pool.imap_unordered(rank_path, generate_paths()):
                    ranking = dict(ranking)
                    row = [path] + [ranking[c] for c in IDENTIFIER.nb_classes]
                    writer.writerow(row)
            else:
                for path, (lang, conf) in pool.imap_unordered(cl_path, generate_paths()):
                    writer.writerow((path, lang, conf))
    else:
        if sys.stdin.isatty():
            # Interactive mode
            while True:
                try:
                    print(">>>", end=' ')
                    text = input()
                except Exception as e:
                    print(e)
                    break
                print(_process(text))
        else:
            # Redirected
            if options.line:
                for line in sys.stdin:
                    print(_process(line))
            else:
                print(_process(sys.stdin.read()))


if __name__ == "__main__":
    main()
