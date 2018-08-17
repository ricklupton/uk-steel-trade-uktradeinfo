Overseas Trade Statistics for HS codes relevant to steel-containing
products, from 1996 to 2017.

The raw trade data is aggregated according to the Biffaward categories to find
steel contents.

## TODO

- The allocations need checking as they were inconsistent between the EU and
  non-EU categories.
- Compare to other trade data (ISSB/worldsteel)

## Data

Data from trade (in goods) between the UK and non-EU countries are collected
from UK Customs import and export entries, and between the UK and other EU
Member States from VAT returns and the HMRC Intrastat survey.

This data was downloaded from the [UK Trade Info "build your own
tables"](https://www.uktradeinfo.com/Statistics/BuildYourOwnTables/Pages/Home.aspx).
It includes all available years (1996 to 2017) and the HS codes relevant to
indirect trade in steel (i.e. trade in goods that contain steel).

## License

The original data from HMRC is available under the [Open Government Licence
v3](https://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/).

## Preparation

Some steps had to be done by hand:

1. Use the [UK Trade Info "build your own
   tables"](https://www.uktradeinfo.com/Statistics/BuildYourOwnTables/Pages/Home.aspx)
   tool to filter relevant HS codes and expand the table to an appropriate level
   of detail.
2. Export the spreadsheet in the `raw_data` folder. 
3. Tweak the spreadsheet by hand to remove merged cells to produce the second
   spreadsheet in the `raw_data` folder.

The datapackage is assembled using the
[datapackage-pipelines](https://github.com/frictionlessdata/datapackage-pipelines)
tool. Run it in this directory to build the datapackage:

```
dpp run ./trade-data
```

To aggregate the trade flows and find iron contents:

```python
python scripts/aggregate_trade_flows.py
```

This creates the `data/imports.csv` and `data/exports.csv` files.
