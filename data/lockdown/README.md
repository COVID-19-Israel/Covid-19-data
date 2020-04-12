# Lockdown table description

This directory contains documantation about our lockdown table. If you are locking for the table itself, you could find it in our website.
We create diffrent tables for each country, with boolean values regarding variable social distancing paramaters.
We then combine them to a single main table.

Files in the directory:
* Lockdown Policy Table - description of each column in the table.
* Lockdown Bibliography.
* diff tables - table that keep records of the changes in the conditions of social isolation policy in the country and in specified region.
* diffs_to_states - python code which takes diff tables and generates lockdown statuses table by dates and calculate each  record's lockdown level.
