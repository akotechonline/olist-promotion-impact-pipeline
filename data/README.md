# Data

## Olist Brazilian E-Commerce Public Dataset

The pipeline reads two files from this dataset. They aren't included in the repository because of their size and license.

1. Download the dataset from Kaggle: <https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce>
   (or with the Kaggle CLI: `kaggle datasets download -d olistbr/brazilian-ecommerce --unzip -p data/raw`)
2. Put the CSV files in `data/raw/`. The pipeline needs these two:

| File | Used for |
|---|---|
| `olist_orders_dataset.csv` | Orders, status and purchase timestamps |
| `olist_order_items_dataset.csv` | Item price and freight per order |

The other Olist files (customers, payments, reviews, products, sellers, geolocation) can sit in the same folder; the pipeline ignores them.

License: [CC BY-NC-SA 4.0](https://creativecommons.org/licenses/by-nc-sa/4.0/), by Olist.

## `campaign_metadata.csv` (included)

A small **synthetic** file created for this project. Olist didn't publish its promotions, so these windows are placed on real holiday periods.

| Column | Meaning |
|---|---|
| `campaign_id` | Campaign key (repeats once per channel) |
| `campaign_name` | Name, for example "Black Friday 2017" |
| `channel` | `email`, `social` or `display` |
| `window_start`, `window_end` | First and last day of the campaign (inclusive) |
| `discount_pct` | Advertised discount |
| `coupon_code` | Coupon code for the campaign |

The repeated rows per channel are deliberate: they reproduce the duplication problem that the staging layer has to solve.
