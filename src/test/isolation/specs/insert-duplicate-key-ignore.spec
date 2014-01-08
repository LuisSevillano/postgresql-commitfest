# INSERT...ON DUPLICATE KEY IGNORE test
#
# This test tries to expose problems with the interaction between concurrent
# sessions during INSERT...ON DUPLICATE KEY IGNORE.
#
# The convention here is that session 1 always ends up inserting, and session 2
# always ends up ignoring.

setup
{
  CREATE TABLE ints (key int primary key, val text);
}

teardown
{
  DROP TABLE ints;
}

session "s1"
setup
{
  BEGIN ISOLATION LEVEL READ COMMITTED;
}
step "ignore1" { INSERT INTO ints(key, val) VALUES(1, 'ignore1') ON DUPLICATE KEY IGNORE; }
step "select1" { SELECT * FROM ints; }
step "c1" { COMMIT; }
step "a1" { ABORT; }

session "s2"
setup
{
  BEGIN ISOLATION LEVEL READ COMMITTED;
}
step "ignore2" { INSERT INTO ints(key, val) VALUES(1, 'ignore2') ON DUPLICATE KEY IGNORE; }
step "select2" { SELECT * FROM ints; }
step "c2" { COMMIT; }
step "a2" { ABORT; }

# Regular case where one session block-waits on another to determine if it
# should proceed with an insert or ignore.
permutation "ignore1" "ignore2" "c1" "select2" "c2"
permutation "ignore1" "ignore2" "a1" "select2" "c2"
