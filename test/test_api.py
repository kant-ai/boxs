import unittest.mock

from datastock.api import load, store
from datastock.data import DataRef
from datastock.errors import DataNotFound, StockNotDefined
from datastock.stock import Stock, _unregister_stock


class TestStore(unittest.TestCase):

    def setUp(self):
        self.stock = Stock('stock-id', None)

    def tearDown(self):
        _unregister_stock(self.stock.stock_id)

    def test_store_resolves_origin_from_where_it_is_called(self):
        self.stock.store = unittest.mock.MagicMock()
        store('my-input', stock=self.stock)
        self.assertEqual(
            self.stock.store.call_args[1]['origin'],
            'test_store_resolves_origin_from_where_it_is_called',
        )

    def test_stock_is_required(self):
        with self.assertRaisesRegex(ValueError, "'stock' must be set."):
            store(None, stock=None)

    def test_stock_is_resolved_from_id(self):
        self.stock.store = unittest.mock.MagicMock()
        store(None, stock='stock-id')
        self.stock.store.assert_called()

    def test_stock_can_be_given_as_object(self):
        self.stock.store = unittest.mock.MagicMock()
        store(None, stock=self.stock)
        self.stock.store.assert_called()

    def test_stock_store_gets_all_arguments(self):
        self.stock.store = unittest.mock.MagicMock()
        store(
            'my-input', 'parent1', 'parent2',
            stock=self.stock,
            name='my-name', origin='my-origin', tags={'my': 'tag'},
            run_id='run-id', meta={'my': 'meta'},
        )
        self.stock.store.assert_called_with(
            'my-input', 'parent1', 'parent2',
            name='my-name', origin='my-origin', tags={'my': 'tag'},
            run_id='run-id', meta={'my': 'meta'},
        )

    def test_stock_store_return_value_is_returned(self):
        self.stock.store = unittest.mock.MagicMock(return_value='My value')
        result = store(None, stock=self.stock)
        self.assertEqual('My value', result)


class TestLoad(unittest.TestCase):

    def setUp(self):
        self.storage = unittest.mock.MagicMock()
        self.stock = Stock('stock-id', self.storage)
        self.data_ref = DataRef('data-id', 'stock-id', 'run-id')

    def tearDown(self):
        _unregister_stock(self.stock.stock_id)

    def test_stock_is_resolved_from_data(self):
        self.stock.load = unittest.mock.MagicMock()
        load(None, self.data_ref)
        self.stock.load.assert_called()

    def test_load_raises_if_stock_does_not_exist(self):
        self.storage.exists = unittest.mock.MagicMock(return_value=False)
        with self.assertRaisesRegex(StockNotDefined, "Stock .* not defined"):
            load(None, DataRef('data-id', 'unknown-stock-id', 'run-id'))

    def test_load_raises_if_data_does_not_exist(self):
        self.storage.exists = unittest.mock.MagicMock(return_value=False)
        with self.assertRaisesRegex(DataNotFound, "Data .* does not exist"):
            load(None, self.data_ref)

    def test_stock_load_gets_all_arguments(self):
        self.stock.load = unittest.mock.MagicMock()
        load('my-output', self.data_ref)
        self.stock.load.assert_called_with('my-output', self.data_ref)

    def test_stock_load_return_value_is_returned(self):
        self.stock.load = unittest.mock.MagicMock(return_value='My value')
        result = load(None, self.data_ref)
        self.assertEqual('My value', result)


if __name__ == '__main__':
    unittest.main()
