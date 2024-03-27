# Combined order-books module

Frequently used tools for researching combined order books from various venues.

## File structure

```txt
|- combinedBooks                         # Directory with modules for collecting and processing order-books.
    |- booksGetter.py                    # Asynchronous data fetcher for concurrent collection of books
    |- coinbaseUtils.py                  # Helper module to clean and structure Coinbase data.
    |- comboBooks.py                     # Combined order books logic.
    |- exchangesData.py                  # Configurations for exchanges.
    |- krakenUtils.py                    # Helper module to clean and structure Kraken data.
    |- orderbook.py                      # Orderbook class for managing order-books.
    |- printColors.py                    # Pretty print order-books (mainly for debugging).
    |- utils.py                          # Helper utilities (timestamps, save data).
|- tests                                 # Tests directory
    |- test_comboBooks.py                # test for comboBooks.py and related logic
```

## License

Copyright (C) 2024 - Chris Liatas

This is free software: you can redistribute it and/or modify it under the terms of the GNU General Public License as published by the Free Software Foundation, either version 3 of the License, or any later version.

This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details.

You should have received a copy of the GNU General Public License along with these files.  If not, see https://www.gnu.org/licenses/.
