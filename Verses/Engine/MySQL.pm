package Verses::Engine::MySQL;
use strict;
use base qw/Verses::Engine/;

__PACKAGE__->register('mysql');

my %legal_ctx_def = 
(
	'create' => "*create->create",
	'alter'  => "*alter",
	"drop"   => "*drop",
	"rename" => "*rename",
);

my %legal_ctx_create = 
(
	'table'  => "table",
	'if_not_exists' => "if_not_exists"
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


sub evaluate {
	my $self = shift @_;
	my $ctx = shift @_;

	my $token = shift @_;
	my @args  = @_;
	my $r_ret = { 'adj' => {}, 'ctx' => $ctx };

	my %legal; my %req_arguments;

	if ($ctx eq 'def') {
		%legal = %legal_ctx_def;
		%req_arguments = %req_arguments_ctx_def;
	} elsif ($ctx eq 'create') {
		%legal = %legal_ctx_create;
		%req_arguments = %req_arguments_ctx_create;		
	}

	if (! $legal{lc $token}) {
		# if ($legal_ctx_def{lc $token} && $legal_ctx_def{lc $token} =~ m/^\*/) {
		# 	my $newCtx = 'def';

		# 	return $self->evaluate($newCtx, $token, @args);
		if ($legal_ctx_def{lc $token}) {
			# Cmd is likely done.
			return 1;
		} else {
			return undef;
		}
	}

	if (int @{$req_arguments{lc $token}} < int @args) {
		print "Args mismatch";
		return undef;
	}

	my $match_kw = $legal{lc $token};
	if ($match_kw =~ m/^\*/) {
		my $action = $match_kw;
		my $ctx    = $match_kw;
		$ctx       =~ s/^\*\w+\-\>//;

		$action =~ s/^\*//;
		$action =~ s/\-\>\w+$//;

		$r_ret->{'action'} = $action;

		if ($ctx) {
			$r_ret->{'ctx'} = $ctx;
		}
	} else {
		$r_ret->{'adj'}{lc $token} = {};
		foreach my $arg_name (@{ $req_arguments{lc $token}}) {
			$r_ret->{'adj'}{lc $token}{$arg_name} = shift @args;
		}

		if (int @args) {
			$r_ret->{'adj'}{lc $token}{"__extra"} = @args;
		}
	}

	return $r_ret;
}

1;