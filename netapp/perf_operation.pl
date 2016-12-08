#===============================================================#
#						    	        #
# $ID$						       		#
#							    	#
# perf_operation.pl				       	     	#
#							    	#
# Sample code for the usage of following APIs: 		     	#
#			perf-object-list-info   		#
#			perf-object-counter-list-info		#
#			perf-object-instance-list-info		#
# 			perf-object-get-instances-iter-*	#
#							    	#
# Copyright 2005 Network Appliance, Inc. All rights	  	#
# reserved. Specifications subject to change without notice.    #
#							    	#
# This SDK sample code is provided AS IS, with no support or    #
# warranties of any kind, including but not limited to          #
# warranties of merchantability or fitness of any kind,         #
# expressed or implied.  This code is subject to the license    #
# agreement that accompanies the SDK.				#
#							    	#
#===============================================================#

require 5.6.1;
use lib "../../../../lib/perl/NetApp";
use lib '/usr/lib64/perl5/vendor_perl/NetApp';
use NaServer;
use NaElement;

# Variable declaration

my $args = $#ARGV + 1;
my $filer = shift;
my $user = shift;
my $pw  = shift;
my $command = shift;


#Invoke routine
main();

sub main()
{
	# check for valid number of parameters
	if ($args < 4)
	{
		print_usage();
	}

	my $s = NaServer->new ($filer, 1, 3);

	$out = $s->set_transport_type(HTTP);
	if (ref ($out) eq "NaElement") {
		if ($out->results_errno != 0) {
			my $r = $out->results_reason();
			print "Connection to $filer failed: $r\n";
			exit (-2);
		}
	}

		$out = $s->set_style(LOGIN);
	if (ref ($out) eq "NaElement") {
		if ($out->results_errno != 0) {
			my $r = $out->results_reason();
			print "Connection to $filer failed: $r\n";
			exit (-2);
		}
	}

		$out = $s->set_admin_user($user, $pw);

	if($command eq "object-list")
	{
		get_object_list($s);
	}
	elsif($command eq "instance-list")
	{
		get_instance_list($s);
	}
	elsif($command eq "counter-list")
	{
		get_counter_list($s);
	}
	elsif($command eq "get-counter-values")
	{
		get_counter_values($s);
	}
	else
	{
		print "Invalid operation\n";
		print_usage();
	}
	exit 0;
}


# perf-object-list-info operation
# Usage: perf_operation.pl <filer> <user> <password> object-list
sub get_object_list
{
	my $s = $_[0];

	if ($args < 4)
	{
		print "Usage: perf_operation.pl <filer> <user> <password> object-list \n";
		exit -1;
	}
	my $in = NaElement->new("perf-object-list-info");


	# Invoke API
	my $out = $s->invoke_elem($in);

	if($out->results_status() eq "failed")
	{
		print($out->results_reason() ."\n");
		exit(-2);
	}

	my $obj_info = $out->child_get("objects");
	my @result = $obj_info->children_get();

	foreach $obj (@result){
		my $obj_name = $obj->child_get_string("name");
		my $priv = $obj->child_get_string("privilege-level");
		print("Object Name = $obj_name Privilege Level = $priv\n");
	}
	print "\n";
}

# perf-object-instance-list-info operation
# Usage: perf_operation.pl <filer> <user> <password> instance-list <objectname>

sub get_instance_list
{
	my $s = $_[0];


	if ($args < 5)
	{
		print "Usage: perf_operation.pl <filer> <user> <password> instance-list <objectname> \n";
		exit -1;
	}

	my $in = NaElement->new("perf-object-instance-list-info");
	$obj_name = @ARGV[0];
	$in->child_add_string("objectname",$obj_name);


	# Invoke API
	my $out = $s->invoke_elem($in);

	if($out->results_status() eq "failed")
	{
		print($out->results_reason() ."\n");
		exit(-2);
	}

	my $inst_info = $out->child_get("instances");
	my @result = $inst_info->children_get();
	foreach $inst (@result){
		my $inst_name = $inst->child_get_string("name");
		print("Instance Name = $inst_name \n");
	}
	print "\n";
}

# perf-object-counter-list-info operation
# Usage: perf_operation.pl <filer> <user> <password> counter-list <objectname>
sub get_counter_list
{
	my $s = $_[0];

	if ($args < 5)
	{
		print "Usage: perf_operation.pl <filer> <user> <password> counter-list <objectname>";
		exit -1;
	}

	my $in = NaElement->new("perf-object-counter-list-info");
	$obj_name = @ARGV[0];
	$in->child_add_string("objectname",$obj_name);

	# Invoke API
	my $out = $s->invoke_elem($in);

	if($out->results_status() eq "failed")
	{
		print($out->results_reason() ."\n");
		exit(-2);
	}

	my $counter_info = $out->child_get("counters");
	my @result = $counter_info->children_get();
	foreach $counter (@result){
		my $counter_name = $counter->child_get_string("name");
		print("Counter Name = $counter_name \t\t\t");
		if ($counter->child_get_string("base-counter")) {
			my $base_counter = $counter->child_get_string("base-counter");
			print("Base Counter = $base_counter\t\t");
		} else {
			print("Base Counter = none\t\t");
		}
		my $privilege_level = $counter->child_get_string("privilege-level");
		print("Privilege_level = $privilege_level\t\t");

		if ($counter->child_get_string("unit")) {
			my $unit = $counter->child_get_string("unit");
			print("Unit = $unit\t\t");
		} else {
			print("Unit = none\t\t");
		}
		print "\n";
	}

}

# perf-object-get-instances-iter-* operation
# Usage: perf_operation.pl <filer> <user> <password> get-counter-values <objectname> [ <counter1> <counter2>...]

sub get_counter_values
{
	my $s = $_[0];
	my $iter_tag;
	my $max_records = 10;
	my $out;
	my $in;
	my $counters;
	my $num_records;

	if ($args < 5)
	{
		print "Usage: perf_operation.pl <filer> <user> <password> get-counter-values <objectname> [ <counter1> <counter2> ...]";
		exit -1;
	}

	$in = NaElement->new("perf-object-get-instances-iter-start");
	$obj_name = @ARGV[0];
	$in->child_add_string("objectname",$obj_name);
	$counters = NaElement->new("counters");

	if ( $args > 5 ) {
		foreach (@ARGV) {
			$counters->child_add_string("counter",$_);
		}
		$in->child_add($counters);
	}

	# Invoke API
	$out = $s->invoke_elem($in);

	if($out->results_status() eq "failed")
	{
		print($out->results_reason() ."\n");
		exit(-2);
	}

	$iter_tag = $out->child_get_string("tag");

	do {
		$in = NaElement->new("perf-object-get-instances-iter-next");
		$in->child_add_string("tag",$iter_tag);
		$in->child_add_string("maximum", $max_records);

		$out = $s->invoke_elem($in);
		if($out->results_status() eq "failed")
		{
			print($out->results_reason() ."\n");
			exit(-2);
		}
		$num_records = $out->child_get_int("records");

		if($num_records != 0) {

			my $instances_list = $out->child_get("instances");
			my @instances = $instances_list->children_get();

			foreach $inst (@instances){
				my $inst_name = $inst->child_get_string("name");
				print ("Instance = $inst_name\n");
				my $counters_list = $inst->child_get("counters");
				my @counters = 	$counters_list->children_get();
				foreach $counter (@counters) {
					my $counter_name = $counter->child_get_string("name");
					my $counter_value = $counter->child_get_string("value");
					print("Counter Name = $counter_name \t Counter Value = $counter_value \n");
				}
				print("\n");
			}
		}

	} while ($num_records != 0);

	$in = NaElement->new("perf-object-get-instances-iter-end");
	$in->child_add_string("tag",$iter_tag);

	$out = $s->invoke_elem($in);
	if($out->results_status() eq "failed")
	{
		print($out->results_reason() ."\n");
		exit(-2);
	}

}



sub print_usage()
{

	print "perf_operation.pl <filer> <user> <password> <operation> <value1>";
	print "[<value2>]\n";
	print "<filer>     -- Filer name\n";
	print "<user>      -- User name\n";
	print "<password>  -- Password\n";
	print "<operation> -- Operation to be performed: \n";
	print "\tobject-list   - Get the list of perforance objects in the system\n";
	print "\tinstance-list - Get the list of instances for a given performance object\n";
	print "\tcounter-list  - Get the list of counters available for a given performance object\n";
	print "\tget-counter-values - Get the values of the counters for all the instances of a performance object\n";
	print "[<value1>]  -- Depends on the operation \n";
	print "[<value2>]  -- Depends on the operation \n";
	exit -1;
}

#=========================== POD ============================#

=head1 NAME

  perf_operation.pl - Displays the usage of perf group APIs

=head1 SYNOPSIS

  perf_operation.pl <filer> <user> <password> <operation> <value1> [<value2>]

=head1 ARGUMENTS

  <filer>
   Filer name.

  <user>
  username.

  <password>
  password.

  <operation>
  Operation to be performed: object-list, instance-list, counter-list, get-counter-values

  <value1>
  Depends on the operation

  [<value2>]
  Depends on the operation

=head1 SEE ALSO

  NaElement.pm, NaServer.pm

=head1 COPYRIGHT

  Copyright 2005 Network Appliance, Inc. All rights
  reserved. Specifications subject to change without notice.

  This SDK sample code is provided AS IS, with no support or
  warranties of any kind, including but not limited to
  warranties of merchantability or fitness of any kind,
  expressed or implied.  This code is subject to the license
  agreement that accompanies the SDK.

=cut

