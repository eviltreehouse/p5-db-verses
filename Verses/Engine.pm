package Verses::Engine;
use strict;

sub new {
	return bless({}, $_[0]);
}

sub register {
	Verses::register_engine($_[1] => $_[0]);
}



1;