import datetime
import unittest

from walrus.streams import TimeSeries
from walrus.tests.base import WalrusTestCase
from walrus.tests.base import db
from walrus.tests.base import stream_test


class TestTimeSeries(WalrusTestCase):
    def setUp(self):
        super(TestTimeSeries, self).setUp()
        for key in ('sa', 'sb', 'sc'):
            db.delete('key')

        self.ts = TimeSeries(db, 'cgabc', {'sa': '0', 'sb': '0', 'sc': '0'},
                             consumer='ts1')

    def _create_test_data(self):
        start = datetime.datetime(2018, 1, 1)
        id_list = []
        keys = ('sa', 'sb', 'sc')
        for i in range(0, 10):
            tskey = getattr(self.ts, keys[i % 3])
            ts = start + datetime.timedelta(days=i)
            id_list.append(tskey.add({'k': '%s-%s' % (keys[i % 3], i)}, id=ts))
        return id_list

    def assertMessages(self, results, expected_ids):
        rdata = [(r.stream, r.timestamp, r.data) for r in results]
        streams = ('sa', 'sb', 'sc')
        edata = [(streams[i % 3], datetime.datetime(2018, 1, i + 1),
                 {'k': '%s-%s' % (streams[i % 3], i)}) for i in expected_ids]
        self.assertEqual(rdata, edata)

    @stream_test
    def test_timeseries_ranges(self):
        docids = self._create_test_data()

        self.ts.create()
        self.assertMessages(self.ts.sa.range(), [0, 3, 6, 9])
        self.assertMessages(self.ts.sb.range(), [1, 4, 7])
        self.assertMessages(self.ts.sc.range(), [2, 5, 8])
        self.assertMessages(self.ts.sc.range(count=2), [2, 5])

        self.assertMessages(self.ts.sa[:docids[4]], [0, 3])
        self.assertMessages(self.ts.sb[:docids[4]], [1, 4])
        self.assertMessages(self.ts.sa[docids[4]:], [6, 9])
        self.assertMessages(self.ts.sb[docids[4]:], [4, 7])

        self.assertMessages([self.ts.sa.get(docids[6])], [6])
        self.assertMessages([self.ts.sa.get(docids[9])], [9])
        self.assertMessages([self.ts.sc.get(docids[5])], [5])
        self.assertTrue(self.ts.sa.get(docids[5]) is None)
        self.assertTrue(self.ts.sb.get(docids[5]) is None)

        # Trim sa down to 2 items.
        self.assertEqual(self.ts.sa.trim(2, False), 2)
        self.assertMessages(self.ts.sa.range(), [6, 9])
        self.assertMessages(self.ts.sa.range(count=1), [6])
        self.assertMessages(self.ts.streams['sa'].range(count=1), [6])

    @stream_test
    def test_timeseries_read(self):
        self._create_test_data()
        self.ts.create()
        self.assertMessages(self.ts.read(count=1), [0, 1, 2])
        self.assertMessages(self.ts.read(count=1), [3, 4, 5])
        self.assertMessages(self.ts.read(count=2), [6, 7, 8, 9])
        self.assertEqual(self.ts.read(), [])

        # Trim the 0-th item off of sa and reset all streams.
        self.ts.sa.trim(3, False)
        self.ts.reset()
        self.assertMessages(self.ts.read(), list(range(1, 10)))

        # Trim the first two items from sc (2 and 5), then set the date so
        # we've read the first item from each queue. Next read will be 4.
        self.ts.sc.trim(1, False)
        self.ts.sb.delete(datetime.datetime(2018, 1, 8))  # Delete item 7.
        self.ts.set_id(datetime.datetime(2018, 1, 4))
        self.assertMessages(self.ts.read(), [4, 6, 8, 9])

    @stream_test
    def test_adding(self):
        self._create_test_data()
        self.ts.create()
        resp = self.ts.set_id()
        self.assertEqual(resp, {'sa': True, 'sb': True, 'sc': True})

        # We can add another record to the max ts if we increment the seq.
        r = self.ts.sa.add({'k': 'sa-10'}, (datetime.datetime(2018, 1, 10), 1))
        self.assertEqual(r, (datetime.datetime(2018, 1, 10), 1))

        r = self.ts.sb.add({'k': 'sb-11'}, (datetime.datetime(2018, 1, 10), 2))
        self.assertEqual(r, (datetime.datetime(2018, 1, 10), 2))

        self.assertEqual(len(self.ts.sa), 5)
        self.assertEqual(len(self.ts.sb), 4)
        self.assertEqual(len(self.ts.sc), 3)

        # Read the newly-added records.
        r10, r11 = self.ts.read()
        self.assertEqual(r10.timestamp, datetime.datetime(2018, 1, 10))
        self.assertEqual(r10.sequence, 1)
        self.assertEqual(r10.stream, 'sa')
        self.assertEqual(r10.data, {'k': 'sa-10'})
        self.assertEqual(r11.timestamp, datetime.datetime(2018, 1, 10))
        self.assertEqual(r11.sequence, 2)
        self.assertEqual(r11.stream, 'sb')
        self.assertEqual(r11.data, {'k': 'sb-11'})

    @stream_test
    def test_timeseries_stream_read(self):
        self._create_test_data()
        self.ts.create()

        # Read two from sa, one from sc, then read 2x from all. Messages that
        # were read will not be re-read.
        self.assertMessages(self.ts.sa.read(count=2), [0, 3])
        self.assertMessages(self.ts.sc.read(count=1), [2])
        self.assertMessages(self.ts.read(count=2), [1, 4, 5, 6, 8, 9])
        self.assertMessages(self.ts.read(count=1), [7])
        for s in (self.ts.sa, self.ts.sb, self.ts.sc):
            self.assertEqual(s.read(), [])

        # Re-set the ID of stream b. Other streams are unaffected, so we just
        # re-read items from stream b.
        self.ts.sb.set_id(datetime.datetime(2018, 1, 4))
        self.assertMessages(self.ts.read(), [4, 7])

        # Re-set the ID of stream a and trim.
        self.ts.sa.set_id('0')
        self.ts.sa.trim(2, False)
        self.assertMessages(self.ts.read(), [6, 9])

    @stream_test
    def test_ack_claim_pending(self):
        self._create_test_data()
        self.ts.create()
        ts1 = self.ts
        ts2 = ts1.consumer('ts2')

        # Read items 0, 1, 3, and 4.
        self.assertMessages(ts1.sa.read(1), [0])
        self.assertMessages(ts2.sb.read(2), [1, 4])
        self.assertMessages(ts2.sa.read(1), [3])

        def assertPending(resp, expected):
            clean = [(r[0][0], r[1], r[3]) for r in resp]
            self.assertEqual(clean, expected)

        # Check pending status. sa was read by ts1 first, then ts2.
        assertPending(ts1.sa.pending(), [
            (datetime.datetime(2018, 1, 1), 'ts1', 1),
            (datetime.datetime(2018, 1, 4), 'ts2', 1)])
        assertPending(ts1.sa.pending(consumer='ts1'), [
            (datetime.datetime(2018, 1, 1), 'ts1', 1)])
        assertPending(ts1.sa.pending(consumer='ts2'), [
            (datetime.datetime(2018, 1, 4), 'ts2', 1)])

        # sb was read by ts2 only.
        assertPending(ts1.sb.pending(), [
            (datetime.datetime(2018, 1, 2), 'ts2', 1),
            (datetime.datetime(2018, 1, 5), 'ts2', 1)])

        # Acknowledge receipt. Although we read the Jan 4th item from "sa"
        # using ts2, we can still acknowledge it from ts1.
        self.assertEqual(ts1.sa.ack(datetime.datetime(2018, 1, 4)), 1)
        self.assertEqual(ts2.sb.ack(datetime.datetime(2018, 1, 2),
                                    datetime.datetime(2018, 1, 5)), 2)

        # Verify pending removed.
        assertPending(ts1.sa.pending(), [
            (datetime.datetime(2018, 1, 1), 'ts1', 1)])
        assertPending(ts2.sb.pending(), [])

        # Claim the first message for consumer ts2.
        resp = ts2.sa.claim(datetime.datetime(2018, 1, 1))
        self.assertMessages(resp, [0])

        # Pending is now marked for ts2, ack'd, and removed.
        assertPending(ts1.sa.pending(), [
            (datetime.datetime(2018, 1, 1), 'ts2', 2)])
        self.assertEqual(ts2.sa.ack(datetime.datetime(2018, 1, 1)), 1)
        assertPending(ts2.sa.pending(), [])

        # Read the rest from consumer ts2 and verify pending.
        self.assertMessages(ts2.read(), [2, 5, 6, 7, 8, 9])
        assertPending(ts2.sa.pending(), [
            (datetime.datetime(2018, 1, 7), 'ts2', 1),
            (datetime.datetime(2018, 1, 10), 'ts2', 1)])
        assertPending(ts2.sb.pending(), [
            (datetime.datetime(2018, 1, 8), 'ts2', 1)])
        assertPending(ts2.sc.pending(), [
            (datetime.datetime(2018, 1, 3), 'ts2', 1),
            (datetime.datetime(2018, 1, 6), 'ts2', 1),
            (datetime.datetime(2018, 1, 9), 'ts2', 1)])

        # Claim the records in sc for ts1.
        resp = ts1.sc.claim(
            datetime.datetime(2018, 1, 3),
            datetime.datetime(2018, 1, 6),
            datetime.datetime(2018, 1, 9))
        self.assertMessages(resp, [2, 5, 8])
