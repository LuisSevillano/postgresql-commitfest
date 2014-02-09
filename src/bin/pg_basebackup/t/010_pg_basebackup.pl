use strict;
use warnings;
use Cwd;
use TestLib;
use Test::More tests => 19;

program_help_ok('pg_basebackup');
program_version_ok('pg_basebackup');
program_options_handling_ok('pg_basebackup');

my $tempdir = tempdir;
start_test_server $tempdir;

command_fails(['pg_basebackup'], 'pg_basebackup needs target directory specified');
command_fails(['pg_basebackup', '-D', "$tempdir/backup"], 'pg_basebackup fails because of hba');

open HBA, ">>$tempdir/pgdata/pg_hba.conf";
print HBA "local replication all trust\n";
print HBA "host replication all 127.0.0.1/32 trust\n";
print HBA "host replication all ::1/128 trust\n";
close HBA;
system_or_bail 'pg_ctl', '-s', '-D', "$tempdir/pgdata", 'reload';

command_fails(['pg_basebackup', '-D', "$tempdir/backup"], 'pg_basebackup fails because of WAL configuration');

open CONF, ">>$tempdir/pgdata/postgresql.conf";
print CONF "max_wal_senders = 10\n";
print CONF "wal_level = archive\n";
close CONF;
restart_test_server;

command_ok(['pg_basebackup', '-D', "$tempdir/backup"], 'pg_basebackup runs');
ok(-f "$tempdir/backup/PG_VERSION", 'backup was created');

command_ok(['pg_basebackup', '-D', "$tempdir/backup2", '--xlogdir', "$tempdir/xlog2"], 'separate xlog directory');
ok(-f "$tempdir/backup2/PG_VERSION", 'backup was created');
ok(-d "$tempdir/xlog2/", 'xlog directory was created');

command_ok(['pg_basebackup', '-D', "$tempdir/tarbackup", '-Ft'], 'tar format');
ok(-f "$tempdir/tarbackup/base.tar", 'backup tar was created');

mkdir "$tempdir/tblspc1";
psql 'postgres', "CREATE TABLESPACE tblspc1 LOCATION '$tempdir/tblspc1';";
psql 'postgres', "CREATE TABLE test1 (a int) TABLESPACE tblspc1;";
command_ok(['pg_basebackup', '-D', "$tempdir/tarbackup2", '-Ft'], 'tar format with tablespaces');
ok(-f "$tempdir/tarbackup2/base.tar", 'backup tar was created');
my @tblspc_tars = glob "$tempdir/tarbackup2/[0-9]*.tar";
note("tablespace tars are @tblspc_tars");
is(scalar(@tblspc_tars), 1, 'one tablespace tar was created');


our $TODO = 'https://commitfest.postgresql.org/action/patch_view?id=1303';

command_fails(['pg_basebackup', '-D', "$tempdir/same", '--xlogdir', "$tempdir/same"],
			  'fails if data and xlog directory are the same');

my $pwd = cwd();
chdir $tempdir or BAIL_OUT("could not chdir to $tempdir: $!");

command_fails(['pg_basebackup', '-D', "$tempdir/same2", '--xlogdir', "same2"],
			  'fails if data and xlog directory are the same, relative xlog directory');
command_fails(['pg_basebackup', '-D', "same3", '--xlogdir', "$tempdir/same3"],
			  'fails if data and xlog directory are the same, relative data directory');

chdir $pwd or BAIL_OUT("could not chdir to $pwd: $!");;

$TODO = undef;
