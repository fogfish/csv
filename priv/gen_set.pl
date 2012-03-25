use POSIX qw/strftime/;

my $n=@ARGV[0] * 1000;  # number of lines
my $d=@ARGV[1];         # number of fields

my $t = time();

while ($n > 0) 
{
	$t++;
	print join(",", (
		"key$n",
		strftime('%Y-%m-%d', localtime($t)),
		strftime('%H:%M:%S.543', localtime($t)),
		map { sprintf("\%.3f", rand(1000)) } (1..$d)
	)) . ",zz\n";
	$n--;
}



