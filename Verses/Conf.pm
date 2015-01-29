package Verses::Conf;
use strict;

my %KNOWN_SETTINGS = ();

my $cache;

sub new {
	if (ref $cache) { return $cache; }

	my $self = bless({}, $_[0]);

	$self->{'conf'} = {};
	$self->{'tagged'} = {};
	$self->{'settings'} = {};

	$self->_load( $_[1] );

	$self->verify_settings();

	$cache = $self;

	return $self;
}

sub reset {
	undef $cache;
	return new(@_);
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

		if ($l =~ m/^\s*\#/ || $l =~ m/^\s+\/\//) {
			# Permit '#' and '//' comments.
			next;
		}

		(my $ele, my $val) = $l =~ m/^([A-Za-z0-9_\-\:]+)\s+(.*?)$/;

		# Wash quotes
		$val =~ s/^(['"]).*?\1//g;

		if (! _valid_element($ele)) {
			print STDERR "Configuration: $l is malformed.\n";
			next;
		}

		my $tag = undef;

		if ($ele =~ m/^set\-(.*?)$/) {
			my $setting = lc $1;

			$self->{'settings'}{$setting} = $val;
			next;
		}

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

		if (length $tag && length $self->{'tagged'}{$tag}{lc $ele}) {
			return $self->{'tagged'}{$tag}{lc $ele};
		} else {
			return $self->{'conf'}{lc $ele};
		}
	} else {
		my $ele = shift @_;
		return $self->{'conf'}{lc $ele};
	}
}

sub get_setting {
	my $self = shift;

	my $s = lc $_[0];

	if (defined $self->{'settings'}{$s}) {
		return $self->{'settings'}{$s};
	}  else {
		return $_[1];
	}
}

sub settings {
	my $self = shift;

	return %{ $self->{'settings'} };
}

sub _valid_element {
	my $ele = shift @_;

	my @valid = (
		'^engine$',
		'^migration_dir$',
		'^set\-[a-zA-Z0-9\-\_]',
		'^(?:[a-zA-Z0-9]+\:)?host$',
		'^(?:[a-zA-Z0-9]+\:)?socket$',
		'^(?:[a-zA-Z0-9]+\:)?username$',
		'^(?:[a-zA-Z0-9]+\:)?password$',
		'^(?:[a-zA-Z0-9]+\:)?database$',
		'^set\-'
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

sub register_setting {
	shift @_ if ref $_[0];
	my $setting = shift @_;

	$KNOWN_SETTINGS{lc $setting} = $_[0];
}

sub verify_settings {
	my $self = shift @_;
	my $has_unknown = 0;

	foreach my $sk (keys %{ $self->{'settings'} }) {
		if (! $KNOWN_SETTINGS{$sk}) {
			print STDERR "[?] setting `$sk` is unknown.\n";
			$has_unknown++;
		}
	}

	if ($has_unknown) {
		# Extra \n for warnings..
		print STDERR "\n";
	}
}

sub known_settings {
	shift @_ if ref $_[0];

	my @ret;

	foreach my $sk (sort keys %KNOWN_SETTINGS) {
		push(@ret, [ $sk, $KNOWN_SETTINGS{$sk} ]);
	}

	return @ret;
}

1;