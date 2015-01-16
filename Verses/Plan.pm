package Verses::Plan;
use strict;

my $ACTION;
my %ADJ;
my $CONTEXT = 'def';

sub new {
	my $self = bless({}, $_[0]);
	$self->{'alive'} = 1;
	return $self;
}

sub up {
	print "no up.";
}

sub down {
	print "no down.";
}

sub db {
	return Verses::db_handle();
}

sub _reset_action {
	undef $ACTION;
	%ADJ    = ();
	$CONTEXT = 'def';
}

sub _engine {
	return $Verses::ENGINE;
}

sub _conf {
	return $Verses::CONF;
}

sub _dbh {
	return $Verses::DBH;
}

sub _kill_plan {
	$_[0]->{'alive'} = 0;
}

sub AUTOLOAD {
	return if $Verses::Plan::AUTOLOAD =~ m/DESTROY$/;

	my $cmd = $Verses::Plan::AUTOLOAD; $cmd =~ s/\w+\:\://g;
	my $plan = shift @_;

	if (! $plan->{'alive'}) {
		return $plan;
	}

	print "[$CONTEXT] INVOKE => " . $cmd . " with " . join(", ", @_) . "\n";

	my $ret = _engine()->evaluate($CONTEXT, $cmd, @_);
	if (! defined $ret) {
		$plan->_kill_plan();
		die "Unexpected: $cmd";
	} elsif (ref $ret) {
		$ACTION = $ret->{'action'} if exists $ret->{'action'};
		$CONTEXT = $ret->{'ctx'} if exists $ret->{'ctx'};

		foreach my $k ( %{ $ret->{'adj'} } ) {
			$ADJ{$k} = $ret->{'adj'}{$k};
		}
	} elsif ($ret == 1) {
		# Cmd end.
		# Actually parse the cmd and do something.
	}

	return $plan; 
}

1;