#!/usr/bin/perl
use strict;
use FindBin qw($Bin);
use lib "$Bin/../perllib";
use ConnectDB qw/connectdb exec_sql get_product/;

my ($yesterday,$today);

my $arg_date = $ARGV[0]; #2019-08-15
my $table = "cdr_agg";

if($arg_date eq "")
{
	($yesterday,$today) = split(/---/,&get_timestamp());
	print "insert yesterday $yesterday 's traffic into $table\n";
}
else
{

	print "insert $arg_date 's traffic into $table\n";
}

my $db = connectdb();

my $now = localtime();
print "===== process at $now =====\n";

my $start_time = time();

####################
## get hash 
####################
#my $r_product = get_product($db);
#my %h_product = %$r_product;
#foreach my $key (keys %h_product)
#{
#	print "$key => $h_product{$key}\n";
#}

my $sql;
my $sql_result;
if($arg_date ne "")
{
	#$sql_result = exec_sql($db,"insert into cdr_agg (date,billing_id,account_id,product_id,country_id,operator_id,provider_id,cpg_id,tpoa,status,sum_split,sum_sell) select date(dbtime) as date,billing_id,account_id,product_id,country_id,operator_id,provider_id,cpg_id,tpoa,status,sum(split), sum(selling_price) from cdr where dbtime >= '$arg_date' and date(dbtime) = '$arg_date' group by date,account_id,billing_id,product_id,country_id,operator_id,provider_id,cpg_id,tpoa,status order by date;");

	$sql = "select date(dbtime) as date,billing_id,account_id,product_id,country_id,operator_id,provider_id,cpg_id,tpoa,status,sum(split) from cdr where dbtime >= '$arg_date' and date(dbtime) = '$arg_date' group by date,account_id,billing_id,product_id,country_id,operator_id,provider_id,cpg_id,tpoa,status order by date;";
}
else #by default yesterday
{
	#$sql_result = exec_sql($db,"insert into cdr_agg (date,billing_id,account_id,product_id,country_id,operator_id,provider_id,cpg_id,tpoa,status,sum_split,sum_sell) select date(dbtime) as date,billing_id,account_id,product_id,country_id,operator_id,provider_id,cpg_id,tpoa,status,sum(split), sum(selling_price) from cdr where dbtime >= current_date - interval '1 day' and dbtime < current_date group by date,account_id,billing_id,product_id,country_id,operator_id,provider_id,cpg_id,tpoa,status order by date;");

	$sql = "select date(dbtime) as date,billing_id,account_id,product_id,country_id,operator_id,provider_id,cpg_id,tpoa,status,sum(split) from cdr where dbtime >= current_date - interval '1 day' and dbtime < current_date group by date,account_id,billing_id,product_id,country_id,operator_id,provider_id,cpg_id,tpoa,status order by date;";
}

my $ref = exec_sql($db, $sql);
my %h_item;
my $count = 0;
my $count_inserted = 0;

while(my ($date,$billing_id,$account_id,$product_id,$cid,$opid,$provider_id,$cpg_id,$tpoa,$status,$sum_qty) = $ref->fetchrow_array)
{
	$count ++;
	### calculate selling price, cdr.selling_price might be wrong
	my $ref2 = exec_sql($db, "select * from pgfunc_get_selling_price_vd($billing_id,$product_id,$cid,$opid,'$date');");
	my ($unit_sell) = $ref2 -> fetchrow_array;
	$ref2 -> finish;
	#print "unit selling price: $unit_sell\n";
	
	my $sum_sell = $unit_sell * $sum_qty;

	my $sql_insert;
	if ($cpg_id ne "")
	{
		$sql_insert = "insert into cdr_agg (date,billing_id,account_id,product_id,country_id,operator_id,provider_id,cpg_id,tpoa,status,sum_split,sum_sell) values ('$date',$billing_id,$account_id,$product_id,$cid,$opid,$provider_id,$cpg_id,'$tpoa','$status',$sum_qty,$sum_sell);";
	}
	else
	{
		$sql_insert = "insert into cdr_agg (date,billing_id,account_id,product_id,country_id,operator_id,provider_id,tpoa,status,sum_split,sum_sell) values ('$date',$billing_id,$account_id,$product_id,$cid,$opid,$provider_id,'$tpoa','$status',$sum_qty,$sum_sell);";

	}
	print "$sql_insert\n";

	my $result = exec_sql($db, $sql_insert);
	my $inserted = $result -> rows();
	#print " -- inserted $inserted\n";

	$count_inserted += $inserted;

}
$ref -> finish;


my $end_time = time();
my $delay = $end_time - $start_time;
print "aggregated $count entries from cdr, inserted $count_inserted into $table, took $delay sec\n";

sub get_timestamp
{
        my $cmd1 = "date +%Y-%m-%d --date=\"yesterday\"";
        my $cmd2 = "date +%Y-%m-%d";

        open(DATE, "$cmd1|");
        my $yesterday = <DATE>;
        chomp($yesterday);
        close(DATE);

        open(DATE, "$cmd2|");
        my $today = <DATE>;
        chomp($today);
        close(DATE);
	

	return "$yesterday---$today";
}

$db ->disconnect();
exit(0);
