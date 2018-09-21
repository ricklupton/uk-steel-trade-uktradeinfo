import pandas as pd
import datapackage

from logzero import logger
import os.path

logger.info('Starting %s', os.path.basename(__file__))

def load_dataframe(filename, resource):
    """Load one table from a datapackage."""
    package = datapackage.Package(filename)
    r = package.get_resource(resource)
    return pd.DataFrame(r.read(), columns=r.headers)


# Load the flows from the datapackage
flows = load_dataframe('datapackage.json', 'flows')
flows['mass'] = flows['mass'].astype(float)  # XXX this is annoying

# Load the allocation table and category iron contents
alloc = pd.read_csv('scripts/hs4_allocations.csv', index_col='HS4')
cats = pd.read_csv('scripts/steel_contents.csv', index_col=0)

# Validate allocations
mult_sums = alloc['multiplier'].groupby('HS4').sum()
assert all((mult_sums == 0) | (mult_sums == 1)), 'Multipliers must sum to 0 or 1'
split_allocs = alloc[(alloc['multiplier'] != 0) &
                     (alloc['multiplier'] != 1) &
                     ~pd.isnull(alloc['multiplier'])]
logger.debug('Split allocations:\n' +
             str(split_allocs[['sector_code', 'stage', 'multiplier']]))


# Join the table and aggregate
table = flows \
    .join(alloc, on='HS4', how='outer') \
    .join(cats, on='sector_code') \
    .dropna(subset=['sector_code'])

# Convert kg to kt and add in the category iron contents. `multiplier` is for
# sharing an HS4-flow between multiple sector-flows
table['mass'] *= table['multiplier'] / 1e6
table['mass_iron'] = table['mass'] * table['iron_content']

table.to_csv('checking_table.csv')

agg = table \
    .groupby(['direction', 'sector_code', 'stage', 'year'], as_index=False) \
    .agg({
        'mass': 'sum',
        'mass_iron': 'sum',
        'sector_group': 'first',  # same in each group of sector_codes
        'sector_name': 'first',
        'iron_content': 'first',
    })


# Pivot into tables for viewing
def pivot_table(direction, value):
    return agg[agg['direction'] == direction] \
        .pivot_table(index=['sector_code', 'stage'],
                     columns='year',
                     values=value)

table_total_exports = pivot_table('export', 'mass')
table_total_imports = pivot_table('import', 'mass')
table_iron_exports = pivot_table('export', 'mass_iron')
table_iron_imports = pivot_table('import', 'mass_iron')

# Save
df = agg.set_index('direction')

df = df[['sector_code', 'sector_group', 'sector_name', 'stage', 'year',
         'iron_content', 'mass', 'mass_iron']]
df['year'] = df['year'].astype(int)
df['iron_content'] = df['iron_content'].round(2)
df['mass'] = df['mass'].round(1)
df['mass_iron'] = df['mass_iron'].round(1)

df.loc['import'].to_csv('data/imports.csv', index=False)
df.loc['export'].to_csv('data/exports.csv', index=False)
