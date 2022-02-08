#!/usr/bin/perl

use strict;
use DBI;
use FindBin qw($Bin);
use Config::Tiny;
use Data::Dumper qw/Dumper/;

my $start_time = time();

my $limit = 80000;
#my $limit = 10;

my $db_cfg = "$Bin/../.config";
print "check $db_cfg\n";

my $cfg = Config::Tiny->read($db_cfg,'utf8');

my $db_database = $cfg->{postgresql}{db};
my $db_host = $cfg->{postgresql}{host};
my $db_user = $cfg->{postgresql}{user};
my $db_passwd = $cfg->{postgresql}{password};

#print Dumper $cfg;

my $db = DBI -> connect("dbi:Pg:database=$db_database;host=$db_host",$db_user,$db_passwd) or die DBI::errstr;

sub execute_sql
{
	my $sql = shift;
	print $sql ."\n";
#	print "Are you sure you want to run this(y/n)?\n";
#	my $confirm = <STDIN>;
#	chomp($confirm);
#	return undef unless $confirm eq "y";
	my $r = $db -> prepare($sql);
 	$r -> execute or die $DBI::errstr;
	return $r;
}

my $now = localtime();
print "===== process at $now =====\n";

my $sql_result;

$sql_result = &execute_sql("insert into cdr_tmp select cdr.id , cdr.dbtime , webuser_id , billing_id, product_id,
				cdr.msgid, cdr.tpoa, cdr.tpoa2, cdr.bnumber,
				country_id, operator_id, dcs, len, udh, xms, 
				notif3.msgid, notif3.status,
				cpg_id, provider_id, notif3.dbtime
			from cdr,notif3 where cdr.msgid=notif3.localid and cdr.bnumber = notif3.bnumber 
			and cdr.status is null
		and cdr.dbtime > current_timestamp - interval '3 days + 1 hour' limit $limit");

print "--" . $sql_result -> rows()."\n";

$sql_result = &execute_sql("delete from cdr where id in (select id from cdr_tmp)");
print "--" . $sql_result -> rows()."\n";

$sql_result = &execute_sql("delete from notif3 where msgid in (select notif3_msgid from cdr_tmp)");
print "--" . $sql_result -> rows()."\n";

my $cond = 1;

### legacy code, on a2p server, cdr_tmp will contain multiple entries for the same id due to concatenated SMS (same localid)
while(defined($cond))
{
	undef($cond);
	my $ref = &execute_sql("select count(*),id from cdr_tmp group by id order by count(*) desc");
	while(my @t = $ref -> fetchrow_array)
	{
		my $count = shift(@t);
		my $id = shift(@t);
		last if $count == 1;
		my $ref2 = &execute_sql("select notif3_dbtime from cdr_tmp where id = $id limit 1");
		@t = $ref2 -> fetchrow_array;
		my $notif3_dbtime = shift(@t);
		&execute_sql("delete from cdr_tmp where id = $id and notif3_dbtime = '$notif3_dbtime'");
		$cond = 1;
	}	
}

$sql_result = &execute_sql("insert into cdr select * from cdr_tmp");
print "--" . $sql_result -> rows()."\n";

&execute_sql("truncate cdr_tmp");

$db -> disconnect();

my $duration = time() - $start_time;

print "DURATION = $duration\n";

