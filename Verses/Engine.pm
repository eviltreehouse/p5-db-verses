package Verses::Engine;
use strict;

sub new {
	return bless({}, $_[0]);
}

sub register {
	Verses::register_engine($_[1] => $_[0]);
}

sub evaluate {
	die ref($Verses::ENGINE) . " did not properly evaluate this migration plan!";
}

sub supported {
	return 1;
}

sub prepare {
	return undef;
}

sub migration_history {
	return undef;
}

sub record_migration {
	return undef;
}

sub rollback_migration {
	return undef;
}

sub get_iteration {
	return 0;
}

sub execute {
	my $self = shift @_;
	my $query = shift;

	my $sth = $Verses::DBH->prepare($query);
	my $ret = $sth->execute();

	return defined $ret ? 1 : undef;
}

sub _ensure {
	my $self = shift;
	my $msg  = shift;
	my $v    = shift;
	my $regex = shift;

	if (! defined $v) {
		die $msg;
	} else {
		if ($regex) {
			if ($v !~ m/$regex/) {
				die $msg;
			}
		}
	}

	return;
}

sub db_handle {
	return $Verses::DBH;
}

sub db_err {
	return "Undetermined error";
}

sub parse {
	die "No parser defined for " . ref($_[0]) . "!";
}

1;