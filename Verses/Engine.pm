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


1;