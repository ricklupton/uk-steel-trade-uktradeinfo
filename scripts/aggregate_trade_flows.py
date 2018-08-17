import pandas as pd
import datapackage

def load_dataframe(filename, resource):
    """Load one table from a datapackage."""
    package = datapackage.Package(filename)
    r = package.get_resource(resource)
    return pd.DataFrame(r.read(), columns=r.headers)

# Load the flows from the datapackage
flows = load_dataframe('datapackage.json', 'flows')
flows['mass'] = flows['mass'].astype(float)  # XXX this is annoying

# Load the allocation table and category iron contents
alloc = pd.read_csv('scripts/hs4_allocations.csv', usecols=['HS4', 'Allocation'], index_col='HS4')
cats = pd.read_csv('scripts/biffaward_categories.csv', index_col=0)

# Join the table and aggregate
agg_total_mass = flows \
    .join(alloc, on='HS4') \
    .groupby(['direction', 'Allocation', 'year']) \
    .sum()

# Convert kg to kt
agg_total_mass['mass'] /= 1e6

# Add in the category iron contents
agg_iron = agg_total_mass \
    .reset_index() \
    .join(cats, on='Allocation')
agg_iron['mass_iron'] = agg_iron['mass'] * agg_iron['iron_content']

# Pivot into tables for viewing
def pivot_table(direction, value):
    return agg_iron[agg_iron['direction'] == direction] \
        .pivot(index='Allocation', columns='year', values=value)

table_total_exports = pivot_table('export', 'mass')
table_total_imports = pivot_table('import', 'mass')
table_iron_exports = pivot_table('export', 'mass_iron')
table_iron_imports = pivot_table('import', 'mass_iron')

# Save
df = agg_iron \
    .rename(columns={'Allocation': 'product_group_id',
                     'name': 'product_group'}) \
    .set_index('direction')


df = df[['product_group_id', 'product_group', 'year',
         'iron_content', 'mass', 'mass_iron']]

df['product_group_id'] = df['product_group_id'].astype(int)

df.loc['import'].to_csv('data/imports.csv', index=False)
df.loc['export'].to_csv('data/exports.csv', index=False)
