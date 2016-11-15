#!/usr/bin/perl -w
use strict;
use DBI;
use Getopt::Std;

# Options:
#  -h HOLDSHELF
#  -o OVERDUES
#  -r RENEWALS
#  -s Send Files

getopts ('hors'); # no d'oeuvres
our ($opt_h, $opt_o, $opt_r, $opt_s);

my $dbserver = "DB_SERVER_IP"; # Your DB Server IP address
my $dbport   = "1032";
my $dbname   = "iii";
my $dblogin  = "shoutbomb"; # the SQL login you want to use
my $dbpw     = 'DB_PASSWORD'; # password for dblogin
(my $sec,my $min,my $hour,my $mday,my $mon,my $year,my $wday,my $yday,my $isdst) = localtime;
$mon++;
$year += 1900;
$mon = sprintf "%02d", $mon; # make the month 2 digits, starting with 0
$mday = sprintf "%02d", $mday; # make the day 2 digits, starting with 0
$hour = sprintf "%02d", $hour; # make the hour 2 digits, starting with 0
$min = sprintf "%02d", $min; # make the min 2 digits, starting with 0
$sec = sprintf "%02d", $sec; # make the sec 2 digits, starting with 0
my $timestamp = $year . $mon . $mday . $hour . $min . $sec;
my $holds_file = "holds" . $timestamp . ".txt";
my $renewals_file = "renewals" . $timestamp . ".txt";
my $overdues_file = "overdues" . $timestamp . ".txt";

my $host = "ftp.shoutbomb.com";
my $port = "990";
my $user = "SHOUTBOMB_LOGIN"; # Your login on the Shoutbomb FTP server
my $password = "SHOUTBOMB_PW"; # Your password on the Shoutbomb FTP server

my $dbh = DBI->connect("dbi:Pg:dbname=$dbname;host=$dbserver;port=$dbport","$dblogin","$dbpw",{AutoCommit=>1,RaiseError=>1,PrintError=>0});

if ($opt_h) {
my $holdshelf = $dbh->prepare("SELECT TRIM (TRAILING '/' from s.content) AS title, to_char(rmi.record_last_updated_gmt,'MM-DD-YYYY') AS last_update, 'i' || rmi.record_num || 'a' AS item_no, 'p' || rmp.record_num || 'a' AS patron_no, h.pickup_location_code AS pickup_location FROM sierra_view.hold AS h RIGHT JOIN sierra_view.patron_record AS p ON ( p.id = h.patron_record_id ) RIGHT JOIN sierra_view.record_metadata AS rmp ON (rmp.id = h.patron_record_id AND rmp.record_type_code = 'p') RIGHT JOIN sierra_view.item_record AS i ON ( i.id = h.record_id ) RIGHT JOIN sierra_view.bib_record_item_record_link AS bil ON ( bil.item_record_id = i.id AND bil.bibs_display_order = 0 ) JOIN sierra_view.bib_record AS b ON ( b.id = bil.bib_record_id ) JOIN sierra_view.subfield as s ON (s.record_id = b.id AND s.marc_tag = '245' and s.tag = 'a') LEFT JOIN sierra_view.varfield AS bt ON ( bt.record_id = b.id AND bt.varfield_type_code = 't' AND bt.occ_num = 0 ) LEFT JOIN sierra_view.varfield AS ic ON ( ic.record_id = i.id AND ic.varfield_type_code = 'c' AND ic.occ_num = 0 ) LEFT JOIN sierra_view.record_metadata AS rmi ON ( rmi.id = i.id AND rmi.record_type_code = 'i') WHERE h.status in ('b','i') AND i.item_status_code = '!' AND h.pickup_location_code Is not null ORDER BY patron_no");

$holdshelf->execute()
   or die "Couldn't execute statement: " . $holdshelf->errstr;

if (! open HOLDSHELF, ">$holds_file") {
   die "Cannot open holds file: $!";
}
while (my @data = $holdshelf->fetchrow_array()) {
   my $notice = join "|", @data;
   print HOLDSHELF "$notice\n";
} #while my data
close HOLDSHELF;

if ($opt_s) {
   system "curl -s -tlsv1 -k -T $holds_file --ftp-ssl-reqd -u $user:$password -Q \"PROT P\" -Q \"CWD /Holds\" ftps://$host:$port";
   system "curl -s -tlsv1 -k -T $holds_file --ftp-ssl-reqd -u $user:$password -Q \"PROT P\" -Q \"CWD /test/Holds\" ftps://$host:$port";
} #if opt_s
} #if opt_h

if ($opt_r) {
   my $renewals = $dbh->prepare("SELECT 'p' || rmp.record_num || 'a' AS patron_no, replace(ib.field_content,' ','') AS item_barcode, s.content AS title, to_char(c.due_gmt,'MM-DD-YYYY') AS due_date, 'i' || rmi.record_num || 'a' AS item_no, round(p.owed_amt,2) AS money_owed, c.loanrule_code_num AS loan_rule, nullif (count(ih.id),0) AS item_holds, nullif (count(bh.id),0) AS bib_holds, c.renewal_count AS renewals, 'b' || rmb.record_num || 'a' AS bib_no FROM sierra_view.checkout AS c RIGHT JOIN sierra_view.patron_record AS p ON ( p.id = c.patron_record_id ) JOIN sierra_view.record_metadata AS rmp ON (rmp.id = c.patron_record_id AND rmp.record_type_code = 'p') JOIN sierra_view.item_record AS i ON ( i.id = c.item_record_id ) JOIN sierra_view.record_metadata AS rmi ON ( rmi.id = i.id AND rmi.record_type_code = 'i') JOIN sierra_view.varfield AS ib ON ( ib.record_id = i.id AND ib.varfield_type_code = 'b') JOIN sierra_view.bib_record_item_record_link AS bil ON ( bil.item_record_id = i.id) JOIN sierra_view.bib_record AS b ON ( b.id = bil.bib_record_id ) JOIN sierra_view.subfield AS s ON ( s.record_id = b.id AND s.marc_tag='245' AND s.tag = 'a') LEFT JOIN sierra_view.hold as bh ON (bh.record_id = b.id) LEFT JOIN sierra_view.hold as ih ON (ih.record_id = i.id and ih.status = '0') LEFT JOIN sierra_view.record_metadata as rmb ON ( rmb.id = b.id AND rmb.record_type_code = 'b') WHERE (c.due_gmt::date - current_date) = 2 GROUP BY 1,2,3,4,5,6,7,10,11 ORDER BY patron_no");

$renewals->execute()
   or die "Couldn't execute statement: " . $renewals->errstr;

if (! open RENEWALS, ">$renewals_file") {
   die "Cannot open renewals file: $!";
}
while (my @ren_data = $renewals->fetchrow_array()) {
   my $notice = join "|", @ren_data;
   print RENEWALS "$notice\n";
}
close RENEWALS;

if ($opt_s) {
   system "curl -s -tlsv1 -k -T $renewals_file --ftp-ssl-reqd -u $user:$password -Q \"PROT P\" -Q \"CWD /Renew\" ftps://$host:$port";
   system "curl -s -tlsv1 -k -T $renewals_file --ftp-ssl-reqd -u $user:$password -Q \"PROT P\" -Q \"CWD /test/Renew\" ftps://$host:$port";
} #if opt_s
} #if opt_r

if ($opt_o) {
   my $overdues = $dbh->prepare("SELECT 'p' || rmp.record_num || 'a' AS patron_no, replace(ib.field_content,' ','') AS item_barcode, s.content AS title, to_char(c.due_gmt,'MM-DD-YYYY') AS due_date, 'i' || rmi.record_num || 'a' AS item_no, round(p.owed_amt,2) AS money_owed, c.loanrule_code_num AS loan_rule, nullif (count(ih.id),0) AS item_holds, nullif (count(bh.id),0) AS bib_holds, c.renewal_count AS renewals, 'b' || rmb.record_num || 'a' AS bib_no FROM sierra_view.checkout AS c RIGHT JOIN sierra_view.patron_record AS p ON ( p.id = c.patron_record_id ) JOIN sierra_view.record_metadata AS rmp ON (rmp.id = c.patron_record_id AND rmp.record_type_code = 'p') JOIN sierra_view.item_record AS i ON ( i.id = c.item_record_id ) JOIN sierra_view.record_metadata AS rmi ON ( rmi.id = i.id AND rmi.record_type_code = 'i') JOIN sierra_view.varfield AS ib ON ( ib.record_id = i.id AND ib.varfield_type_code = 'b') JOIN sierra_view.bib_record_item_record_link AS bil ON ( bil.item_record_id = i.id) JOIN sierra_view.bib_record AS b ON ( b.id = bil.bib_record_id ) JOIN sierra_view.subfield AS s ON ( s.record_id = b.id AND s.marc_tag='245' AND s.tag = 'a') LEFT JOIN sierra_view.hold as bh ON (bh.record_id = b.id) LEFT JOIN sierra_view.hold as ih ON (ih.record_id = i.id and ih.status = '0') LEFT JOIN sierra_view.record_metadata as rmb ON ( rmb.id = b.id AND rmb.record_type_code = 'b') WHERE (current_date - c.due_gmt::date) >9 AND (current_date - c.due_gmt::date) <31 GROUP BY 1,2,3,4,5,6,7,10,11 ORDER BY patron_no");

$overdues->execute()
   or die "Couldn't execute statement: " . $overdues->errstr;

if (! open OVERDUES, ">$overdues_file") {
   die "Cannot open overdues file: $!";
}
while (my @od_data = $overdues->fetchrow_array()) {
   my $notice = join "|", @od_data;
   print OVERDUES "$notice\n";
}
close OVERDUES;

if ($opt_s) {
   system "curl -s -tlsv1 -k -T $overdues_file --ftp-ssl-reqd -u $user:$password -Q \"PROT P\" -Q \"CWD /Overdue\" ftps://$host:$port";
   system "curl -s -tlsv1 -k -T $overdues_file --ftp-ssl-reqd -u $user:$password -Q \"PROT P\" -Q \"CWD /test/Overdue\" ftps://$host:$port";
} #if opt_s
} #if opt_o


