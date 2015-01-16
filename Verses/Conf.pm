package Verses::Conf;
use strict;

sub new {
	my $self = bless({}, $_[0]);

	$self->{'conf'} = {};
	$self->{'tagged'} = {};

	$self->_load( $_[1] );

	return $self;
}

sub _load {
	my $self = shift @_;
	my $fn = shift @_;

	open CONF, $fn or return;

	while(<CONF>) {
		chomp;
		my $l = $_;

		$l =~ s/^\s+//g;
		$l =~ s/\s+$//g;

		if (! length $l) {
			next; 
		}

		if ($l =~ m/^\s*\#/) {
			next;
		}

		(my $ele, my $val) = $l =~ m/^([A-Za-z0-9_\:]+)\s+(.*?)$/;

		if (! _valid_element($ele)) {
			print STDERR "Configuration: $l is malformed.\n";
			next;
		}

		my $tag = undef;

		if ($ele =~ m/\:/) {
			($tag, $ele) = $ele =~ m/^([a-zA-Z0-9]+)\:(.*?)$/;
		}

		if ($tag && (! $self->{'tagged'})) {
			$self->{'tagged'}{$tag} = {};
		} 

		if ($tag) {
			$self->{'tagged'}{$tag}{lc $ele} = $val;
		} else {
			$self->{'conf'}{lc $ele} = $val;
		}
	}
	close(CONF);
}

sub get {
	my $self = shift;
	if (int @_ == 2) {
		my $tag = shift @_;
		my $ele = shift @_;

		if (length $self->{'tagged'}{lc $ele}) {
			return $self->{'tagged'}{lc $ele};
		} else {
			return $self->{'conf'}{lc $ele};
		}
	} else {
		my $ele = shift @_;
		return $self->{'conf'}{lc $ele};
	}
}

sub _valid_element {
	my $ele = shift @_;

	my @valid = (
		'^engine$',
		'^migration_dir$',
		'^(?:[a-zA-Z0-9]+\:)?host$',
		'^(?:[a-zA-Z0-9]+\:)?socket',
		'^(?:[a-zA-Z0-9]+\:)?username',
		'^(?:[a-zA-Z0-9]+\:)?password',
		'^(?:[a-zA-Z0-9]+\:)?database'
	);

	my $okay = 0;
	foreach my $regex (@valid) {
		if ($ele =~ m/$regex/) {
			$okay = 1;
			last;
		}
	}

	return $okay;
}



1;