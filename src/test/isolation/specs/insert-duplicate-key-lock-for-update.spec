# INSERT...ON DUPLICATE KEY LOCK FOR UPDATE test
#
# This test tries to expose problems with the interaction between concurrent
# sessions during INSERT...ON DUPLICATE LOCK FOR UPDATE.

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
step "lock1" { INSERT INTO ints(key, val) VALUES(1, 'lock1') ON DUPLICATE KEY LOCK FOR UPDATE; }
step "select1" { SELECT * FROM ints; }
step "c1" { COMMIT; }
step "a1" { ABORT; }

session "s2"
setup
{
  BEGIN ISOLATION LEVEL READ COMMITTED;
}
step "lock2" { INSERT INTO ints(key, val) VALUES(1, 'lock2') ON DUPLICATE KEY LOCK FOR UPDATE; }
step "select2" { SELECT * FROM ints; }
step "c2" { COMMIT; }
step "a2" { ABORT; }

# Regular case where one session block-waits on another to determine if it
# should proceed with an insert or lock.
permutation "lock1" "lock2" "c1" "select2" "c2"
permutation "lock1" "lock2" "a1" "select2" "c2"
