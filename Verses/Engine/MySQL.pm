package Verses::Engine::MySQL;
use strict;
use base qw/Verses::Engine/;

require DBI;
require Data::Dumper;

my $MIG_TABLENAME = "_verses_migrations";

Verses::Conf::register_setting('mysql-tabletype', 'default tabletype to use');
Verses::Conf::register_setting('mysql-collation', 'collation type');

my %grammar = (
	'kw_def' => {
		'create' => "*create->create",
		'alter'  => "*alter->alter",
		"drop"   => "*drop->drop",
		"rename" => "*rename",
		'raw'    => '*raw!'		
	},
	'kw_create' => {
		'table'  => "table!",
		'if_not_exists' => "if_not_exists"
	},
	'kw_alter'  => {
		'table' => "table!"
	},	
	'kw_drop'   => {
		'table'  => "table!",
		'if_exists' => "if_exists"
	},
	'kw_tbuild' => {
		'add' => '*addcol',
		'modify' => '*modcol',
		'drop'   => '*dropcol!',
		'drop_index' => '*dropidx!',
		'as'     => 'as!',
		'the'    => 'the!',
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
		'new_unique' => 'new_unique#idxt',
		'new_index'  => 'new_index#idxt'		
	},
	'args_def' => {
		'rename' => [qw/tableSrc tableDest/],
		'raw'    => [qw/q/]			
	},
	'args_create' => {
		'table'  => [qw/tableName tableBuilder/],
	},
	'args_drop' => {
		'table'  => [qw/tableName/]
	},
	'args_alter' => {
		'table' => [qw/tableName tableBuilder/]
	},
	'args_tbuild' => {
		'as'  => [qw/colName/],
		'the' => [qw/colName/],
		'float'    => [qw/floatDef/],
		'char'     => [qw/siz/],
		'varchar'  => [qw/siz/],
		'default'  => [qw/defVal/],
		'new_unique'  => [qw/cols/],
		'new_index' => [qw/cols/],
		'drop'  => [qw/e/],
		'drop_index' => [qw/e/]
	}
);

$grammar{'kw_talter'} = $grammar{'kw_tbuild'};
$grammar{'args_talter'} = $grammar{'args_tbuild'};

sub grammar {
	return \%grammar;
}

sub parse {
	# $plan, $CONTEXT, $ACTION, \%ADJ
	my $self   = shift;
	my $plan   = shift;
	my $ctx    = shift;
	my $action = shift;
	my $r_adj  = shift;

	#print "[.] MySQL [$ctx] ACTION: " . $action . " with " . join(", ", map { $_ . " = " . $r_adj->{$_} } keys %$r_adj) . "\n";

	if ($action eq 'create') {
		return $self->_action_create($plan, %$r_adj);
	} elsif ($action eq 'addcol') {
		my $def = $self->_action_createColumnOrIndex($plan, $ctx, %$r_adj);
		return $def;
	} elsif ($action eq 'modcol') {
		my $def = $self->_action_modifyColumn($plan, %$r_adj);
		return $def;
	} elsif ($action eq 'dropcol' || $action eq 'dropidx') {
		my $def = $self->_action_dropColumnOrIndex($plan, $action, %$r_adj);
		return $def;	
	} elsif ($action eq 'alter') {
		return $self->_action_alter($plan, %$r_adj);
	} elsif ($action eq 'drop') {
		return $self->_action_drop($plan, %$r_adj);
	} elsif ($action eq 'raw') {
		my $q = $self->_action_raw($plan, %$r_adj);
		return $q;
	} else {
		die "Unhandled action '$action'";
	}
}

sub _action_raw {
	my $self = shift;
	my $plan = shift;
	my %adj  = @_;

	#print Data::Dumper->Dump([ \%adj ]);

	my $query;

	$self->_ensure("No query specified", $adj{'raw'}{'q'});

	# Count interpolation uses and verify
	my $int_count = 0;
	my $q = $adj{'raw'}{'q'};

	while ($q =~ m/\?/g) {
		$int_count += 1;
	}

	if ($int_count > 0) {
		$self->_ensure("Interpolated values required, not provided", $adj{'raw'}{"__extra"});

		my @ex = @{ $adj{'raw'}{'__extra'} };

		if (int @ex != $int_count) {
			$self->_ensure("Interpolated value count mismatch", undef);
		}

		while ($q =~ m/\?/) {
			my $nv = shift @ex;
			$nv = _q($nv);
			$q =~ s/\?/$nv/;
		}
	}

	return $q;
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

	my @idxs;
	foreach (@cols) {
		if ($_ =~ m/^\:\I\:/) {
			# Index definition, move to end.
			push (@idxs, $_);
		}
	}

	@cols = grep { $_ =~ m/^\:\I\:/ ? undef : $_ } @cols;

	push(@q, $adj{'table'}{'tableName'}, "(");
	push(@q, join(",\n", @cols));
	push(@q, join(",\n", @idxs));
	push(@q, ")");

	return join(" ", @q);
}

sub _action_alter {
	my $self = shift;
	my $plan = shift;
	my %adj  = @_;

	$self->_ensure('FAILED', $adj{'table'}); $self->_ensure('Bad Table Name', $adj{'table'}{'tableName'}, '^[A-Za-z0-9_]+$'); $self->_ensure('FAILED ModifySub', $adj{'table'}{'tableBuilder'});

	my $modifier = $adj{'table'}{'tableBuilder'};
	$plan->_reset_action();
	$plan->_queue_actions();
	$plan->_set_context('talter');
	&{$modifier}($plan);
	my @changes = $plan->_queued();

	@changes = map { "ALTER TABLE " . $adj{'table'}{'tableName'} . " " . $_  } @changes;

	return @changes;
}

sub _action_drop {
	my $self = shift @_;
	my $plan = shift @_;
	my %adj  = @_;

	my @q = qw/DROP TABLE/;

	$self->_ensure('FAILED', $adj{'table'}); $self->_ensure('Bad Table Name', $adj{'table'}{'tableName'}, '^[A-Za-z0-9_]+$');

	if ($adj{'if_exists'}) {
		push(@q, qw/IF EXISTS/);
	}

	push (@q, $adj{'table'}{'tableName'});

	return join(" ", @q);
}

sub _action_createColumnOrIndex {
	my $self = shift;
	my $plan = shift;
	my $ctx  = shift;
	my %adj  = @_;

	#print Data::Dumper->Dump([ \%adj ]);
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

		if ($ctx eq 'talter') {
			# Append for proper query syntax
			unshift(@def, "ADD COLUMN");
		}

	} elsif ($adj{'idxt'}) {
		#print Data::Dumper->Dump([ \%adj ]);

		my %valid_idx = ('new_unique' => 1, 'new_index' => 1);
		my %idx_title = ('new_unique' => 'UNIQUE KEY' => 'new_index' => 'INDEX');

		$self->_ensure('Invalid index/key type specified', $valid_idx{ $adj{'idxt'} });
		$self->_ensure('No column(s) specified', $adj{ $adj{'idxt'} });
		$self->_ensure('No column(s) specified', $adj{ $adj{'idxt'} }{'cols'});
		$self->_ensure('Columns should be ARRAYREF', ref $adj{ $adj{'idxt'} }{'cols'} eq 'HASH' ? undef : 0);

		my @cols = ref $adj{ $adj{'idxt'} }{'cols'} eq 'ARRAY' ? @{ $adj{ $adj{'idxt'} }{'cols'} } : $adj{ $adj{'idxt'} }{'cols'};

		my @safe_cols = map { s/[^A-Za-z0-9_]//g; $_; } @cols;

		my $idx_name = $adj{'as'}{'colName'};
		if (! length $idx_name) {
			$idx_name = _idx_auto_name($adj{'idxt'}, @safe_cols);
		}

		if ($ctx eq 'talter') {
			push(@def, "ADD", $idx_title{$adj{'idxt'}}, $idx_name);
			push(@def, "(", join(", ", @safe_cols), ")");
		} else {
			push(@def, $idx_title{ $adj{'idxt'} }, $idx_name);
			push(@def, "(", join(", ", @safe_cols), ")");
			unshift(@def, ":I:");
		}

		#die "Got an INDEX!!!";
	} else {
		die "Unknown column entity";
	}

	return join(" ", @def);
}

sub _action_dropColumnOrIndex {
	my $self = shift;
	my $plan = shift;
	my $action = shift;

	my %adj  = @_;

	#print Data::Dumper->Dump( [$action, \%adj ] );

	my %e = ( 'dropcol' => 'COLUMN', 'dropidx' => 'INDEX' );
	my %ek = ('dropcol' => 'drop', 'dropidx' => 'drop_index');

	$self->_ensure('Invalid drop action', $e{ $action });
	$self->_ensure('No column/index specified', $adj{ $ek{$action} });
	$self->_ensure('No column/index specified', $adj{ $ek{$action} }{'e'});

	my @def;

	push(@def, "DROP", $e{$action}, $adj{ $ek{$action} }{'e'});

	return join(" ", @def);
}

sub _action_modifyColumn {
	my $self = shift;
	my $plan = shift;
	my %adj = @_;

	#print Data::Dumper->Dump( [\%adj ] );

	$self->_ensure('No target specified.', $adj{'the'}); $self->_ensure('No target specified.', $adj{'the'}{'colName'});

	my @def;

	push(@def, "MODIFY COLUMN");
	push(@def, $adj{'the'}{'colName'});

	push(@def, $self->_action_createColumnOrIndex($plan, '', %adj));

	return join(" ", @def);
}

sub _q {
	return Verses::db_handle->quote( $_[0] );
}

sub _idx_auto_name {
	my $idx_t = shift @_;
	my @cols = @_;

	my $idx_name = '';
	$idx_name = join("_", @cols);

	$idx_name .= "_idx"  if $idx_t eq 'add_index';
	$idx_name .= "_uidx" if $idx_t eq 'add_unique';

	return $idx_name;
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

	my $q = "SELECT iteration,migration FROM $MIG_TABLENAME ORDER BY iteration, migration;";
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

sub rollback_migration {
	my $self      = shift @_;
	my $iteration = shift @_;
	my @migs      = @_;

	my $dbh = Verses::db_handle;

	foreach my $mig (@migs) {
		my $q = "DELETE FROM $MIG_TABLENAME WHERE iteration=? AND migration=?;";
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