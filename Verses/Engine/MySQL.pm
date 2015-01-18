package Verses::Engine::MySQL;
use strict;
use base qw/Verses::Engine/;

require DBI;

my %legal_ctx_def = 
(
	'create' => "*create->create",
	'alter'  => "*alter",
	"drop"   => "*drop",
	"rename" => "*rename",
);

my %legal_ctx_create = 
(
	'table'  => "table!",
	'if_not_exists' => "if_not_exists"
);

my %legal_ctx_tbuild = 
(
	'add' => '*addcol',
	'as' => 'as!',
	'int' => 'int',
	'smallint' => 'int',
	'bigint'   => 'int',
	'float'    => 'float',
	'char'     => 'char',
	'varchar'  => 'varchar',
	'text'     => 'text',
	'mediumtext' => 'mediumtext',
	'largetext'  => 'largetext',
	'datetime' => 'datetime',
	'signed'   => 'signed',
	'nullable' => 'nullable',
	'default'  => 'default',
	'primary'  => 'primary',
	'auto_increment' => 'auto_increment',
	'unique'   => 'unique',
	'indexed'  => 'indexed',
	'add_unique' => 'add_unique',
	'add_index'  => 'add_index'
);

my %req_arguments_ctx_def =
(
	'create' => [],
	'alter'  => [qw/tableName/],
	'drop'   => [qw/tableName/],
	'rename' => [qw/tableSrc tableDest/]
);

my %req_arguments_ctx_create = (
	'table'  => [qw/tableName tableBuilder/],
	'if_not_exists' => []
);

my %req_arguments_ctx_tbuild =
(
	'as' => [qw/colName/],
	'int' => [],
	'smallint' => [],
	'bigint'   => [],
	'float'    => [qw/floatDef/],
	'char'     => [qw/siz/],
	'varchar'  => [qw/siz/],
	'text'     => [],
	'mediumtext' => [],
	'largetext'  => [],
	'datetime' => [],
	'signed'   => [],
	'nullable' => [],
	'default'  => [qw/defVal/],
	'primary'  => [],
	'unique'   => [],
	'indexed'    => [],
	'add_unique'  => [qw/col/],
	'add_index' => [qw/col/]
);

sub evaluate {
	my $self = shift @_;
	my $ctx = shift @_;

	my $token = shift @_;
	my @args  = @_;
	my $r_ret = { 'adj' => {}, 'ctx' => $ctx, 'done' => 0 };

	my %legal; my %req_arguments;

	if ($ctx eq 'def') {
		%legal = %legal_ctx_def;
		%req_arguments = %req_arguments_ctx_def;
	} elsif ($ctx eq 'create') {
		%legal = %legal_ctx_create;
		%req_arguments = %req_arguments_ctx_create;		
	} elsif ($ctx eq 'tbuild') {
		%legal = %legal_ctx_tbuild;
		%req_arguments = %req_arguments_ctx_tbuild;		
	}

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

	if ($match_kw =~ m/^\*/) {
		my $action = $match_kw;
		$ctx;

		if ($action =~ m/\-\>/) {
			# Context shift.
			$ctx = $action;
			$ctx =~ s/^\*\w+\-\>//;
		}

		$action =~ s/^\*//;
		$action =~ s/\-\>\w+$//;

		$r_ret->{'action'} = $action;

		if ($ctx) {
			$r_ret->{'ctx'} = $ctx;
		}
	} elsif ($match_kw =~ m/\!$/) {
		$r_ret->{'done'} = 1;
	}

	if (ref $req_arguments{$token} eq 'ARRAY') {
		if (int @{$req_arguments{$token}} > 0 || int @args) {
			$r_ret->{'adj'}{$token} = {};
		}

		foreach my $arg_name (@{ $req_arguments{$token} }) {
			my $a = shift @args;
			print "--- $token: $arg_name => " . (ref($a) ? "REF" : $a) ."\n";
			$r_ret->{'adj'}{$token}{$arg_name} = $a;
		}
	} else {
		#print "---- $token has no args.\n";
	}

	if (int @args) {
		$r_ret->{'adj'}{$token} = {} if ! defined $r_ret->{'adj'}{$token};
		$r_ret->{'adj'}{$token}{"__extra"} = @args;
	}


	return $r_ret;
}

sub parse {
	# $plan, $CONTEXT, $ACTION, \%ADJ
	my $self   = shift;
	my $plan   = shift;
	my $ctx    = shift;
	my $action = shift;
	my $r_adj  = shift;

	print "[.] MySQL [$ctx] ACTION: " . $action . " with " . join(", ", map { $_ . " = " . $r_adj->{$_} } keys %$r_adj) . "\n";

	if ($action eq 'create') {
		return $self->_action_create($plan, %$r_adj);
	} elsif ($action eq 'addcol') {
		return "addcol: " . $r_adj->{'as'}{'colName'};
	} else {
		die "Unhandled action '$action'";
	}
}

sub _action_create {
	my $self = shift;
	my $plan = shift;
	my %adj  = @_;

	my @q = qw/CREATE/;

	$self->_ensure('FAILED', $adj{'table'}); $self->_ensure('Bad Table Name', $adj{'table'}{'tableName'}, '^[A-Za-z0-9_]+$'); $self->_ensure('FAILED TBUILD', $adj{'table'}{'tableBuilder'});

	my @cols;
	if (! ref($adj{'table'}{'tableBuilder'}) eq 'CODE') {
		die "Invalid Table Builder.";
	}

	my $builder = $adj{'table'}{'tableBuilder'};
	$plan->_reset_action();
	$plan->_queue_actions();
	$plan->_set_context('tbuild');
	print "---+ calling builder...\n";
	&{$builder}($plan);
	@cols = $plan->_queued();
	print "+--- builder done.\n";


	if ($adj{'if_not_exists'}) {
		push(@q, qw/IF NOT EXISTS/);
	}

	push(@q, $adj{'table'}{'tableName'}, "(");
	push(@q, map { "$_;" } @cols);
	push(@q, ")");

	return join(" ", @q);
}

sub db_handle {
	my $self = shift;
	my $conf = shift;

	# We need the following details:
	# host / socket
	# username
	# password
	# database

	my %handle_opts = (
		AutoCommit => 1,
		RaiseError => 0,
		PrintError => 0
	);

	my $host = $conf->get($Verses::TAG ? ($Verses::TAG => 'host') : 'host');
	my $socket = $conf->get($Verses::TAG ? ($Verses::TAG => 'socket') : 'socket');
	my $username = $conf->get($Verses::TAG ? ($Verses::TAG => 'username') : 'username');
	my $password = $conf->get($Verses::TAG ? ($Verses::TAG => 'password') : 'password');
	my $database = $conf->get($Verses::TAG ? ($Verses::TAG => 'database') : 'database');

	my $target = $host || $socket;
	if (! length $target) {
		return undef;
	}

	my $dbi = join(":", 'dbi', 'mysql', $database, $target);

	my $dbh = DBI->connect($dbi, $username, $password, \%handle_opts);

	return undef unless $dbh && $dbh->ping;
	return $dbh;
}

sub db_err {
	my $self = shift;
	my $handle = shift;

	return $handle->errstr;
}

__PACKAGE__->register('mysql');

1;