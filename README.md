# Introduction
A temporal graph generator used for TGraph benchmark.

# Usage
Install Pycharm, open this project, then configure a python interpreter(python 3.9 is recommended.).

# Parameters Introduction
## time
start: the start time, the format obeys the ISO 8601 rule.

end: the end time.

step: Update the temporal data every step seconds.

## entity:
total: The total number of entities(vertex or edge).

update_proportion: The entity with update_proportion percentage will be updated at each transaction.

temporal_property_number: The number of temporal property.

temporal_property_type: true means int and false means float.

temporal_property_update_proportion: Every temporal property with temporal_property_update_proportion percentage will be updated at each update transaction.


