package Verses v0.1.0;
use strict;

require File::Spec;
require Cwd;
require Verses::Conf;

require Verses::Engine::MySQL;
require Verses::Plan;
#require Verses::Engine::SQLite;

my @SUPPORTED_ENGINES = ();
my %ENGINES;

our $CONF;
our $DBH;
our $ENGINE;
our $TAG;

my $verbose = 0;

sub migrate {
	conf();
	my $to_run;

	if (int @_) {
		foreach my $arg (@_) {
			if ($arg =~ m/^\d+$/) {
				$to_run = $arg;
			} elsif ($arg =~ m/^\w+$/) {
				$TAG = $arg;
			}
		}
	}

	sanity_check();

	my @to_run = ();
	my %has_run = map { $_->[1] => $_->[0] } $ENGINE->migration_history();

	foreach my $planFile (get_plans()) {
		my $planClass = $planFile;
		$planClass =~ s/\.v$//g;

		if (! $has_run{$planClass}) {
			push (@to_run, $planClass);
		}
	}

	if ($to_run > 0 && $to_run < int @to_run) {
		@to_run = $to_run[0..($to_run-1)];
	}

	if (! int @to_run) {
		print "[.] Nothing to migrate.\n";
		exit 0;
	}

	my $iteration = $ENGINE->get_iteration();

	foreach my $planStub (@to_run) {
		my $planClass = "Verses\:\:Plan\:\:$planStub";

		do File::Spec->catfile( get_plan_dir(), "$planStub.v" );
		if ($@) {
			print "[X] $planStub\n";
			print $@;
		} else {
			my $plan = $planClass->new();

			#$SIG{__DIE__} = sub { print "[X] KA-BOOM! " . @_; print "*Shrugs*"; }; 
			
			eval {
				$plan->up();
			};

			#$SIG{__DIE__} = undef;

			if ($@) {
				print "[X] $planStub: " . _swave($@) . "\n";
			} else {
				$ENGINE->record_migration($iteration => $planStub);
				print "[^] $planStub\n";
			}
		}
	}
}

sub rollback {
	my $to_roll = 1;

	if (int @_) {
		foreach my $arg (@_) {
			if ($arg =~ m/^\d+$/) {
				$to_roll = $arg;
			} elsif ($arg =~ m/^\w+$/) {
				$TAG = $arg;
			}
		}
	}

	sanity_check();

	my @to_roll = ();
	my @to_roll_iters = ();
	my @history = $ENGINE->migration_history();
	my $last_iter = 0;

	foreach (@history) {
		$last_iter = $_->[0] if $_->[0] > $last_iter;
	}

	foreach my $i (0..($to_roll-1)) {
		my $nv = $last_iter - $i;
		if ($nv > 0) {
			push(@to_roll_iters, $nv);
		}
	}

	my %rb_iter;

	foreach my $iter (reverse sort @to_roll_iters) {
		# Make sure we run these backwards.
		foreach my $h (reverse @history) {
			if ($h->[0] == $iter) {
				push (@to_roll, $h->[1]);
				$rb_iter{$h->[1]} = $iter;
			}
		}
	}

	if (! int @to_roll) {
		print "[.] Nothing to rollback.\n";
	}

	#print "[>] Rolling back ITERs: " . join(",", reverse sort @to_roll_iters) . " (plans: " . join(",", @to_roll) . ")\n";
	foreach my $planStub (@to_roll) {
		my $planClass = "Verses\:\:Plan\:\:$planStub";

		do File::Spec->catfile( get_plan_dir(), "$planStub.v" );
		if ($@) {
			print "[X] $planStub\n";
			print $@;
		} else {
			my $plan = $planClass->new();

			eval {
				$plan->down();
			};

			if ($@) {
				print "[X] $planStub: " . _swave($@) . "\n";
			} else {
				$ENGINE->rollback_migration($rb_iter{$planStub} => $planStub);
				print "[v] $planStub\n";
			}
		}
	}

	exit 0;
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

sub init {
	my $engine = shift @_;
	my $mig_dir = shift @_; $mig_dir ||= 'db/migrations';

	if (! valid_engine($engine)) {
		print "[!] '$engine' is not a supported database engine.\n";
		print "[!] Supported engines are: " . join(', ', @SUPPORTED_ENGINES) . "\n";
		exit 1;
	}

	my $verses_file = File::Spec->catfile(Cwd::getcwd(), '.db-verses');
	if (-f $verses_file) {
		print "[!] $verses_file already exists! Edit it or remove it and re-run 'init' again.\n";
		exit 1;
	}

	if (! -w Cwd::getcwd()) {
		print "[!] You do not have permission to write to this directory.\n";
		exit 1;
	}

	open VF, ">", $verses_file or exit 1;
	print VF _skel_conf($engine, $mig_dir);
	close(VF);

	print "[.] $verses_file created.\n";
	print "[.] You may configure it according to your particular database configuration.\n";

	if (! -d $mig_dir) {
		`mkdir -p $mig_dir 2>/dev/null`;
		if ($? == 0) {
			print "[.] $mig_dir initalized.\n";
		} else {
			print "[!] Failed to create $mig_dir -- please create by hand.\n";
		}
	}

	exit 0;
}

sub sanity_check {
	conf();
	if (! ref $CONF) {
		print "[!] Unable to load Verses configuration file (.db-verses). Have you run 'init'?\n";
		exit 1;
	}

	load_engine( $CONF->get('engine') );
	if (! $ENGINE) {
		print "[!] Unable to load migration engine: " . $CONF->get('engine') . "\n";
		print "[!] Ensure the library files for Verses are up-to-date and that your engine is supported\n";
		print "[!] Supported engines are: " . join(", ", sort @SUPPORTED_ENGINES) . "\n";
		exit 1;
	}

	if (! db_handle()) {
		print "[!] Unable to establish a connection to the database!\n";
		exit 1;
	}

	if (! defined $ENGINE->prepare()) {
		print "[!] Database engine could not initialize itself for migration status storage. Ensure this user can " .
			  "INSERT INTO the migration table and/or this user can CREATE tables.\n";
		exit 1;
	}
}

sub db_handle {
	if (ref $DBH) {
		return $DBH;
	} else {
		$DBH = $ENGINE->db_handle( $CONF );
		return $DBH;
	}
}

sub load_engine {
	my $engine = shift @_;

	if (! $ENGINES{$engine}) {
		return undef;
	}

	$ENGINE = $ENGINES{$engine}->new();
}

sub register_engine {
	my $tag = shift @_;
	my $class = shift @_;

	$verbose && print "register_engine() $tag => $class\n";

	$ENGINES{$tag} = $class;

	@SUPPORTED_ENGINES = sort keys %ENGINES;
}

sub get_plan_dir {
	return File::Spec->catdir(Cwd::getcwd(), "db");
}

sub get_plans {
	my $plan_dir = get_plan_dir();
	my @plans = sort map { chomp; $_ } `ls $plan_dir`;

	return @plans;
}

sub conf {
	if (ref $CONF) { return $CONF; }

	my $dir = Cwd::getcwd();
	my $mfile;

	$dir =~ s/\\//
	;
	my $quit = 0;
	until ($quit) {
		if (-f File::Spec->catfile($dir, '.db-verses')) {
			$mfile = File::Spec->catfile($dir, '.db-verses');
		}

		my @dirs = File::Spec->splitdir($dir);
		shift @_;

		if ($dirs[int @dirs-1] eq '') {
			$quit++;
		} else {
			pop @dirs;
			$dir = File::Spec->catdir(@dirs);
		}
	}

	if (-f $mfile) {
		$CONF = new Verses::Conf($mfile);
		return $CONF;
	} else {
		return undef;
	}
}

sub valid_engine {
	my $e = shift @_;

	#my $match = int grep { lc($e) eq $_ ? $_ : undef } @SUPPORTED_ENGINES;
	#return $match > 0 ? 1 : 0;

	return exists $ENGINES{$e};
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
	my \$plan = shift \@_;

}

sub down {
	# Define your de-migration plan HERE:
	my \$plan = shift \@_;

}
SKEL
;
}

sub _skel_conf {
	my $engine  = shift @_;
	my $mig_dir = shift @_;
	return <<CONF
engine $engine
migration_dir $mig_dir
host db-host-name
#socket socket-file-name
username user-name
password password
database database-name

CONF
;
}

sub _swave {
	my $msg = shift @_;	#return $msg;
	
	$msg =~ s/at [A-Za-z0-9_\.\/\\]+ line \d+\.$//;
	return $msg;
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
