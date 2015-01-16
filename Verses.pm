package Verses v0.1.0;
use strict;

require File::Spec;
require Cwd;

sub migrate {
	foreach my $planFile (get_plans()) {
		my $planClass = $planFile;
		$planClass =~ s/\.v$//g;
		$planClass = "Verses\:\:Plan\:\:$planClass";

		do File::Spec->catfile( get_plan_dir(), $planFile );
		if ($@) {
			print "[X] $planFile\n";
		} else {
			my $plan = $planClass->new();
			$plan->up();
			print "[+] $planFile\n";
		}
	}
}

sub rollback {
	print "Rollback!\n";
}

sub plan {
	my $planId = _legal_plan_id( shift @_ );
	my $timecode = timecode();

	$planId = "${timecode}_${planId}";
	my $plan = _skel_plan($planId);

	my $fn = "${planId}.v";

	open PLAN, ">", File::Spec->catfile( get_plan_dir(), $fn );
	print PLAN $plan;
	close(PLAN);

	print "[.] $fn created.\n";
}

sub get_plan_dir {
	return File::Spec->catdir(Cwd::getcwd(), "db");
}

sub get_plans {
	my $plan_dir = get_plan_dir();
	my @plans = sort map { chomp; $_ } `ls $plan_dir`;

	return @plans;
}

sub timecode {
	my $now = time;
	my @today = localtime;
	my $yr = $today[5] - 70;
	$now -= ($yr * 365) * 86400;

	return $now;
}

sub _legal_plan_id {
	my $id = shift @_;

	$id =~ s/[^A-Za-z0-9_]/_/g;

	return $id;
}

sub _skel_plan {
	my $planId = shift @_;
	return <<SKEL
package Verses\:\:Plan\:\:$planId;
use strict;
use base qw/Verses\:\:Plan/;
sub new { my \$c = shift \@_; return \$c->SUPER::new(\@_); }	

sub up {
	# Define your migration plan HERE:

}

sub down {
	# Define your de-migration plan HERE:

}
SKEL
;
}


{
	no strict 'refs';

	#
	# Assemble cmds in main scope to simply cmdline perl -e invocation
	#
	foreach my $cmd (qw/migrate rollback plan init/) {
		*{'main::' . $cmd} = *{'Verses::' . $cmd};
	}
}

1;
