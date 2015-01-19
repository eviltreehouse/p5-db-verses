package Verses::Engine::MySQL;
use strict;
use base qw/Verses::Engine/;

require DBI;
require Data::Dumper;

my $MIG_TABLENAME = "_verses_migrations";

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
	'int' => 'int#dt',
	'smallint' => 'smallint#dt',
	'bigint'   => 'bigint#dt',
	'float'    => 'float#dt',
	'char'     => 'char#dt',
	'varchar'  => 'varchar#dt',
	'text'     => 'text#dt',
	'mediumtext' => 'mediumtext#dt',
	'largetext'  => 'largetext#dt',
	'datetime' => 'datetime#dt',
	'signed'   => 'signed',
	'nullable' => 'nullable',
	'default'  => 'default',
	'primary'  => 'primary',
	'auto_increment' => 'auto_increment',
	'unique'   => 'unique',
	'indexed'  => 'indexed',
	'add_unique' => 'unique#idxt',
	'add_index'  => 'index#idxt'
);

my %req_arguments_ctx_def =
(
	'alter'  => [qw/tableName/],
	'drop'   => [qw/tableName/],
	'rename' => [qw/tableSrc tableDest/]
);

my %req_arguments_ctx_create = (
	'table'  => [qw/tableName tableBuilder/],
);

my %req_arguments_ctx_tbuild =
(
	'as' => [qw/colName/],
	'float'    => [qw/floatDef/],
	'char'     => [qw/siz/],
	'varchar'  => [qw/siz/],
	'default'  => [qw/defVal/],
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

		$action =~ s/^\*//;
		$action =~ s/\-\>\w+$//;

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
		$r_ret->{'adj'}{$token} = {} if ! $r_ret->{'adj'}{$token};
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
		my $def = $self->_action_createColumnOrIndex($plan, %$r_adj);
		print "DEF: $def\n";
		#return "addcol: " . $r_adj->{'as'}{'colName'};
		return $def;
	} else {
		die "Unhandled action '$action'";
	}
}

sub _action_create {
	my $self = shift;
	my $plan = shift;
	my %adj  = @_;

	my @q = qw/CREATE TABLE/;

	$self->_ensure('FAILED', $adj{'table'}); $self->_ensure('Bad Table Name', $adj{'table'}{'tableName'}, '^[A-Za-z0-9_]+$'); $self->_ensure('FAILED TBUILD', $adj{'table'}{'tableBuilder'});

	my @cols;
	if (! ref($adj{'table'}{'tableBuilder'}) eq 'CODE') {
		die "Invalid Table Builder.";
	}

	my $builder = $adj{'table'}{'tableBuilder'};
	$plan->_reset_action();
	$plan->_queue_actions();
	$plan->_set_context('tbuild');
	#print "---+ calling builder...\n";
	&{$builder}($plan);
	@cols = $plan->_queued();
	#print "+--- builder done.\n";


	if ($adj{'if_not_exists'}) {
		push(@q, qw/IF NOT EXISTS/);
	}

	push(@q, $adj{'table'}{'tableName'}, "(");
	push(@q, join(",\n", @cols));
	push(@q, ")");

	return join(" ", @q);
}

sub _action_createColumnOrIndex {
	my $self = shift;
	my $plan = shift;
	my %adj  = @_;

	print Data::Dumper->Dump([ \%adj ]);
	my @def;
	my $colName = $adj{'as'}{'colName'};
	if ($adj{'dt'}) {
		my $dt = $adj{'dt'};
		push (@def, $colName);
		if (ref $adj{$dt}) {
			push(@def, "$dt(" . $adj{$dt}{'siz'} . ")");
		} else {
			push (@def, $dt);
		}

		if ($dt =~ m/text$/) {
			# Text columns are NULL.
			$adj{'nullable'} = 1;
		}

		if (! $adj{'nullable'} && ! $adj{'auto_increment'}) {
			push (@def, "NOT NULL");
			if (! $adj{'auto_increment'}) {
				if (! $adj{'default'}) {
					die "No default defined for NULLABLE column: $colName";
				} else {
					if (! defined $adj{'default'}{'defVal'}) {
						die "No default defined for NULLABLE column: $colName";
					} else {
						push (@def, "DEFAULT " . _q($adj{'default'}{'defVal'}));
					}
				}
			}
		}

		if ($adj{'auto_increment'}) { push (@def, "AUTO_INCREMENT"); }

		if ($adj{'primary'}) { push(@def, "PRIMARY KEY"); }
		if ($adj{'unique'}) { push(@def, "UNIQUE"); }

	} elsif ($adj{'idxt'}) {
		# @TODO
	} else {
		die "Unknown column entity";
	}

	return join(" ", @def);
}

sub _q {
	return Verses::db_handle->quote( $_[0] );
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

sub prepare {
	my $self = shift;

	my $dbh = Verses::db_handle;
	if (! $dbh) { return undef; }

	my $q = "SHOW TABLES LIKE '$MIG_TABLENAME';";
	my $sth = $dbh->prepare($q);
	$sth->execute();

	if (! $sth->rows) {
		return $self->_create_migration_table();
	} else {
		return 1;
	}
}

sub _create_migration_table {
	my $self = shift;

	my $query = <<QUERY
CREATE TABLE $MIG_TABLENAME (
	iteration int,
	migration varchar(255),
	migrated datetime,
	INDEX (iteration)
)	
QUERY
;

	my $sth = Verses::db_handle->prepare($query);
	if (! $sth->execute()) {
		return undef;
	} else {
		return 1;
	}
}

sub get_iteration {
	my $self = shift;

	my $q = "SELECT MAX(iteration) FROM $MIG_TABLENAME;";
	my $sth = Verses::db_handle->prepare($q);
	$sth->execute();

	my $max_iter = $sth->fetchrow_array();

	return $max_iter + 1;
}

sub migration_history {
	my $self = shift;

	my $q = "SELECT iteration,migration FROM $MIG_TABLENAME ORDER BY iteration;";
	my $sth = Verses::db_handle->prepare($q);
	$sth->execute();

	my @history;
	while (my $row = $sth->fetchrow_hashref()) {
		push (@history, [ $row->{'iteration'} => $row->{'migration'} ]);
	}
	$sth->finish;

	return @history;
}

sub record_migration {
	my $self      = shift @_;
	my $iteration = shift @_;
	my @migs      = @_;

	my $dbh = Verses::db_handle;

	foreach my $mig (@migs) {
		my $q = "INSERT INTO $MIG_TABLENAME (iteration, migration, migrated) VALUES(?,?,NOW());";
		my $sth = $dbh->prepare($q);
		if (! $sth->execute($iteration, $mig)) {
			return undef;
		}
	}

	return 1;
}

sub supported {
	eval {
		require DBD::mysql;

		#delete $INC{'DBD/mysql.pm'};
	};

	return length $@ ? undef : 1;
}

BEGIN {
	__PACKAGE__->register('mysql') if __PACKAGE__->supported;
}
1;