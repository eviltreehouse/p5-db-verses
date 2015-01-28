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

	if ($Verses::TRY_MODE) {
		$query =~ s/\n/ /g;
		print "[>> " . $Verses::TRY_STATE . "] " . $query . "\n";
	} else {
		my $sth = $Verses::DBH->prepare($query);
		my $ret = $sth->execute();

		return defined $ret ? 1 : undef;
	}
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

sub grammar {
	return {};
}

sub evaluate {
	my $self = shift @_;
	my $ctx = shift @_;

	my $token = shift @_;
	my @args  = @_;
	my $r_ret = { 'adj' => {}, 'ctx' => $ctx, 'done' => 0 };

	my %legal; my %req_arguments;

	%legal = %{ $self->grammar->{'kw_' . $ctx} } if ref $self->grammar->{'kw_' . $ctx };
	%req_arguments = %{ $self->grammar->{'args_' . $ctx} } if ref $self->grammar->{'args_' . $ctx };

	my $token = lc $token;

	if (! $legal{$token}) {
		# Unknown command.
		return undef;
	}

	if (ref $req_arguments{$token} eq 'ARRAY') {
		if (int @{$req_arguments{$token}} < int @args) {
			print "Args mismatch";
			return undef;
		}
	}

	my $match_kw = $legal{$token};
	my $adj_mark;

	if ($match_kw =~ m/^\*/) {
		my $action = $match_kw;
		$ctx;

		if ($action =~ m/\-\>/) {
			# Context shift.
			$ctx = $action;
			$ctx =~ s/^\*\w+\-\>//;
		}

		if ($match_kw =~ /\#(\w+)$/) {
			$adj_mark = $1;
		}

		if ($match_kw =~ m/\!$/) {
			$r_ret->{'done'} = 1;
		}

		$action =~ s/^\*//;
		$action =~ s/\-\>\w+$//;
		$action =~ s/\!$//;

		$r_ret->{'action'} = $action;

		if ($ctx) {
			$r_ret->{'ctx'} = $ctx;
		}
	} elsif ($match_kw =~ m/\!$/) {
		$r_ret->{'done'} = 1;
	} elsif ($match_kw =~ /\#(\w+)$/) {
		$adj_mark = $1;
	}

	if (ref $req_arguments{$token} eq 'ARRAY') {
		if ($adj_mark) {
			$r_ret->{'adj'}{$adj_mark} = $token;
		}

		if (int @{$req_arguments{$token}} > 0 || int @args) {
			$r_ret->{'adj'}{$token} = {};
		}

		foreach my $arg_name (@{ $req_arguments{$token} }) {
			my $a = shift @args;
			#print "--- $token: $arg_name => " . (ref($a) ? "REF" : $a) ."\n";
			$r_ret->{'adj'}{$token}{$arg_name} = $a;
		}
	} else {
		#print "---- $token has no args.\n";
		my $k = $adj_mark ? $adj_mark : $token;
		my $v = $adj_mark ? $token : 1;
		$r_ret->{'adj'}{$k} = $v;
	}

	if (int @args && !$adj_mark) {
		print "Extra arguments for $token... " . join(",",@args);
		$r_ret->{'adj'}{$token} = {} if ! $r_ret->{'adj'}{$token};
		print $r_ret->{'adj'}{$token} = {};
		$r_ret->{'adj'}{$token}{"__extra"} = \@args;
	}


	return $r_ret;
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