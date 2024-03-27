# Combined order-books module

Frequently used tools for researching combined order books from various venues.

## File structure

```txt
|- combinedBooks                         # Directory with modules for collecting and processing order-books.
    |- booksGetter.py                    # Asynchronous data fetcher for concurrent collection of books
    |- CoinbaseUtils.py                  # Helper module to clean and structure Coinbase data.
    |- exchangesData.py                  # Configurations for exchanges.
    |- krakenUtils.py                    # Helper module to clean and structure Kraken data.
    |- orderbook.py                      # Orderbook class for managing order-books.
    |- printColors.py                    # Pretty print order-books (mainly for debugging).
    |- utils.py                          # Helper utilities (timestamps, save data).
```
