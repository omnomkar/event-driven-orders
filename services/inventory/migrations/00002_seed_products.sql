-- +goose Up
-- Example catalog for local runs and load tests. Most products have deep
-- stock; sku-limited and sku-soldout exist to exercise out-of-stock paths.
INSERT INTO products (id, name, unit_price_cents, currency, available) VALUES
    ('sku-keyboard', 'Mechanical keyboard',       8999, 'USD', 1000000),
    ('sku-mouse',    'Wireless mouse',            2999, 'USD', 1000000),
    ('sku-monitor',  '27-inch monitor',          24999, 'USD', 1000000),
    ('sku-headset',  'Noise-cancelling headset',  7999, 'USD', 1000000),
    ('sku-cable',    'USB-C cable',                999, 'USD', 1000000),
    ('sku-limited',  'Limited edition desk mat',  2499, 'USD',       5),
    ('sku-soldout',  'Sold-out stand',            4999, 'USD',       0);

-- +goose Down
DELETE FROM products WHERE id IN
    ('sku-keyboard', 'sku-mouse', 'sku-monitor', 'sku-headset', 'sku-cable', 'sku-limited', 'sku-soldout');
