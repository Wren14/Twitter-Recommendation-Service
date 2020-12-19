package edu.cmu.cc.group.q3;

import java.util.HashSet;

public class BannedWords {

	public static HashSet<String> getBannedWords() {
		
		HashSet<String> bannedWordsSet = new HashSet<>(bannedWords.length);
		
		for (String bannedWord : bannedWords) {
			
			
			
			bannedWordsSet.add(ROT13Util.decrypt(bannedWord));
		}
		
		return bannedWordsSet;
	}
	
	private final static String[] bannedWords = new String[] {
			"15619ppgrfg",
			"4e5r",
			"5u1g",
			"5uvg",
			"a1ttn",
			"a1ttre",
			"abournq",
			"abowbpxl",
			"abowbxrl",
			"ahgfnpx",
			"ahzoahgf",
			"anmv",
			"avtt3e",
			"avtt4u",
			"avttn",
			"avttnf",
			"avttnm",
			"avttnu",
			"avttre",
			"avttref",
			"bzt",
			"c0ea",
			"cbbc",
			"ceba",
			"cevpx",
			"cevpxf",
			"chffl",
			"chfflf",
			"chffr",
			"chffv",
			"chffvrf",
			"chor",
			"cnja",
			"cravf",
			"cravfshpxre",
			"cubarfrk",
			"cuhd",
			"cuhpx",
			"cuhx",
			"cuhxf",
			"cuhxrq",
			"cuhxvat",
			"cuhxxrq",
			"cuhxxvat",
			"cvff",
			"cvffbss",
			"cvffre",
			"cvffref",
			"cvffrf",
			"cvffsyncf",
			"cvffva",
			"cvffvat",
			"cvtshpxre",
			"cvzcvf",
			"dhrre",
			"erpghz",
			"evzwnj",
			"evzzvat",
			"fangpu",
			"fbabsnovgpu",
			"fchax",
			"fpebghz",
			"fpebgr",
			"fpebng",
			"fpuybat",
			"frk",
			"frzra",
			"fu1g",
			"fuvg",
			"fuvgf",
			"fuvggl",
			"fuvggre",
			"fuvggref",
			"fuvggrq",
			"fuvggvat",
			"fuvggvatf",
			"fuvgqvpx",
			"fuvgr",
			"fuvgrl",
			"fuvgrq",
			"fuvgshpx",
			"fuvgshyy",
			"fuvgurnq",
			"fuvgvat",
			"fuvgvatf",
			"fxnax",
			"fyhg",
			"fzhg",
			"fzrtzn",
			"gbffre",
			"gheq",
			"gj4g",
			"gjhag",
			"gjhagre",
			"gjng",
			"gjnggl",
			"gjngurnq",
			"grrgf",
			"gvg",
			"gvggljnax",
			"gvgglshpx",
			"gvggvrf",
			"gvggvrshpxre",
			"gvgjnax",
			"gvgshpx",
			"i14ten",
			"i1ten",
			"ihyin",
			"intvan",
			"ivnten",
			"j00fr",
			"jgss",
			"jnat",
			"jnax",
			"jnaxl",
			"jnaxre",
			"juber",
			"juber4e5r",
			"juberfuvg",
			"jubernany",
			"jubne",
			"n55",
			"nahf",
			"nany",
			"nefr",
			"nff",
			"nffenz",
			"nffjubyr",
			"nffshpxre",
			"nffshxxn",
			"nffub",
			"o00of",
			"o17pu",
			"o1gpu",
			"obare",
			"obbbbbbbof",
			"obbbbbof",
			"obbbbof",
			"obbbof",
			"obbo",
			"obbof",
			"obvbynf",
			"obyybpx",
			"obyybx",
			"oernfgf",
			"ohaalshpxre",
			"ohgg",
			"ohggcyht",
			"ohggzhpu",
			"ohprgn",
			"ohttre",
			"ohyyfuvg",
			"ohz",
			"onfgneq",
			"onyyf",
			"onyyfnpx",
			"orfgvny",
			"orfgvnyvgl",
			"ornfgvny",
			"ornfgvnyvgl",
			"oryyraq",
			"ovgpu",
			"ovngpu",
			"oybbql",
			"oybjwbo",
			"oybjwbof",
			"p0px",
			"p0pxfhpxre",
			"pahg",
			"pbba",
			"pbk",
			"pbpx",
			"pbpxf",
			"pbpxfhpx",
			"pbpxfhpxf",
			"pbpxfhpxre",
			"pbpxfhpxrq",
			"pbpxfhpxvat",
			"pbpxfhxn",
			"pbpxfhxxn",
			"pbpxsnpr",
			"pbpxurnq",
			"pbpxzhapu",
			"pbpxzhapure",
			"pbx",
			"pbxfhpxn",
			"pbxzhapure",
			"penc",
			"phaavyvathf",
			"phag",
			"phagf",
			"phagyvpx",
			"phagyvpxre",
			"phagyvpxvat",
			"phavyvathf",
			"phavyyvathf",
			"phz",
			"phzf",
			"phzfubg",
			"phzzre",
			"phzzvat",
			"plnyvf",
			"ploreshp",
			"ploreshpx",
			"ploreshpxre",
			"ploreshpxref",
			"ploreshpxrq",
			"ploreshpxvat",
			"pnecrgzhapure",
			"pnjx",
			"puvax",
			"pvcn",
			"py1g",
			"pyvg",
			"pyvgbevf",
			"pyvgf",
			"q1px",
			"qbaxrlevoore",
			"qbbfu",
			"qbtshpxre",
			"qbttva",
			"qbttvat",
			"qhpur",
			"qlxr",
			"qnza",
			"qvax",
			"qvaxf",
			"qvefn",
			"qvpx",
			"qvpxurnq",
			"qvyqb",
			"qvyqbf",
			"qypx",
			"rwnphyngr",
			"rwnphyngrf",
			"rwnphyngrq",
			"rwnphyngvat",
			"rwnphyngvatf",
			"rwnphyngvba",
			"rwnxhyngr",
			"s4aal",
			"sbbx",
			"sbbxre",
			"shk",
			"shk0e",
			"shpx",
			"shpxf",
			"shpxjuvg",
			"shpxjvg",
			"shpxn",
			"shpxre",
			"shpxref",
			"shpxrq",
			"shpxurnq",
			"shpxurnqf",
			"shpxva",
			"shpxvat",
			"shpxvatf",
			"shpxvatfuvgzbgureshpxre",
			"shpxxxqngggovgpuuuu",
			"shpxzr",
			"shqtrcnpxre",
			"shx",
			"shxf",
			"shxjuvg",
			"shxjvg",
			"shxre",
			"shxxre",
			"shxxva",
			"snaalshpxre",
			"snaalsyncf",
			"snall",
			"snt",
			"sntbg",
			"sntbgf",
			"sntf",
			"snttbg",
			"snttf",
			"snttvat",
			"snttvgg",
			"sphx",
			"sphxre",
			"sphxvat",
			"srpx",
			"srpxre",
			"srypuvat",
			"sryyngr",
			"sryyngvb",
			"svatreshpx",
			"svatreshpxf",
			"svatreshpxre",
			"svatreshpxref",
			"svatreshpxrq",
			"svatreshpxvat",
			"svfgshpx",
			"svfgshpxf",
			"svfgshpxre",
			"svfgshpxref",
			"svfgshpxrq",
			"svfgshpxvat",
			"svfgshpxvatf",
			"synatr",
			"tbngfr",
			"tbqqnza",
			"tnatonat",
			"tnatonatf",
			"tnatonatrq",
			"tnlfrk",
			"ubeal",
			"ubeavrfg",
			"uber",
			"ubgfrk",
			"ubne",
			"ubner",
			"ubre",
			"ubzb",
			"uneqpberfrk",
			"urfur",
			"uryy",
			"wnc",
			"wnpxbss",
			"wrex",
			"wrexbss",
			"wvfz",
			"wvm",
			"wvmm",
			"wvmz",
			"xabo",
			"xaboraq",
			"xabornq",
			"xaborq",
			"xabournq",
			"xabowbpxl",
			"xabowbxrl",
			"xbaqhz",
			"xbaqhzf",
			"xbpx",
			"xhavyvathf",
			"xhz",
			"xhzf",
			"xhzzre",
			"xhzzvat",
			"xnjx",
			"y3vgpu",
			"y3vpu",
			"yhfg",
			"yhfgvat",
			"ynovn",
			"yznb",
			"yzsnb",
			"z0s0",
			"z0sb",
			"z45greongr",
			"zbgunshpx",
			"zbgunshpxf",
			"zbgunshpxn",
			"zbgunshpxnf",
			"zbgunshpxnm",
			"zbgunshpxre",
			"zbgunshpxref",
			"zbgunshpxrq",
			"zbgunshpxva",
			"zbgunshpxvat",
			"zbgunshpxvatf",
			"zbgureshpx",
			"zbgureshpxf",
			"zbgureshpxre",
			"zbgureshpxref",
			"zbgureshpxrq",
			"zbgureshpxva",
			"zbgureshpxvat",
			"zbgureshpxvatf",
			"zbgureshpxxn",
			"zbs0",
			"zbsb",
			"zhgun",
			"zhgunshpxxre",
			"zhgunsrpxre",
			"zhgure",
			"zhgureshpxre",
			"zhss",
			"zn5greo8",
			"zn5greongr",
			"znfbpuvfg",
			"znfgheongr",
			"znfgreo8",
			"znfgreong",
			"znfgreong3",
			"znfgreongr",
			"znfgreongvba",
			"znfgreongvbaf"
	};
	
}
