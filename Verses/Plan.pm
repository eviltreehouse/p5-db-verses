package Verses::Plan;
use strict;

my $ACTION;
my %ADJ;
my $CONTEXT = 'def';
my $CTXLOCK = 0;

my @AQUEUE = ();

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
	$CONTEXT = $CTXLOCK ? $CONTEXT : 'def';
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

sub _queue_actions {
	my $self = shift @_;
	$CTXLOCK = 1;
	@AQUEUE = ();
	$self->{'_queue'} = 1;
}

sub _queued {
	my $self = shift @_;
	my @copy = @AQUEUE;
	$self->{'_queue'} = 0;
	@AQUEUE = ();

	$CTXLOCK = 0;
	return @copy;
}

sub _set_context {
	$CONTEXT = $_[1];
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
		foreach my $k ( keys %{ $ret->{'adj'} } ) {
			#print "$k => " . $ret->{'adj'}{$k};
			$ADJ{$k} = $ret->{'adj'}{$k};
		}

		if ($ret->{'done'}) {
			# Parse/Execute the command..
			my $ret = _engine()->parse($plan, $CONTEXT, $ACTION, \%ADJ);
			if ($ret) {
				if ($plan-{"_queue"}) {
					push (@AQUEUE, $ret);
				} else {
					print "RUN QUERY: $ret\n";
					my $qret = _engine()->execute($ret);
					if (! defined $qret) {
						die _engine->db_err( _engine->db_handle );
					}
				}
			}

			# And do the next one..
			$plan->_reset_action();
		} else {
			$ACTION = $ret->{'action'} if exists $ret->{'action'};
			$CONTEXT = $ret->{'ctx'} if exists $ret->{'ctx'};
		}
	}

	return $plan; 
}

1;