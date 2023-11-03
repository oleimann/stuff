import java.io.* ;
import java.util.* ;

public class MirrorSpeeds implements Comparable<MirrorSpeeds>
{
	final static String CLASSNAME = "MirrorSpeeds" ;
	final static String CLASSVER  = "1.1";
	final static String MIRRORSPEEDS_INFO =
		"[java] "+CLASSNAME+" version "+CLASSVER+" written by Olaf Leimann with GNU license";

	final static String VERSION_HISTORY = 
		"\nVersion history:\n"+
			"-\tv1.1 fixes \"November logs\"(*) and adds resume count.\n"+
			"\t*) a date/time calculation issue across month boundaries.";

	final static String    USAGE = "Usage: java MirrorSpeeds [<options>] <SnapMirror-Logfile> [<SnapMirror-Logfile> ...]" ;
	final static String [] OPTIONS = {
		"d",  "debug",    "debug output on stderr",
		"ld", "logdebug", "debug output on stdout",
		"h",  "help",     "shows this output",
		"nf", "nofail",   "skip failed operations",
		"xml","audit-mlog","Use audit-mlog extract to add extra info to each update request"
	};

	final static String HMS = "##:##:##" ; 

	final static String SUCCESS_MESSAGE = "Success" ;
	final static String FAILURE_MESSAGE = "Failure" ;
	final static String    INFO_MESSAGE = "Info:" ;
	
	final static String [] KNOWN_OPERATIONS = {
		"ManualUpdate", "ScheduledUpdate", "DelayedUpdate", "Initialize", "StorageEfficientSnapshotRotation"
	};
	final static String [] SHOW_OPERATIONS = {
		" Triggered ", " CDOTsched ", " OTDelayed "
	};
	final static int HANDLED_TYPES = SHOW_OPERATIONS.length ;
	
	static boolean knownOperation(String op) {
		if(op != null && op.length() > 0) 
			for(int i=KNOWN_OPERATIONS.length-1;i>=0;i--)
				if(KNOWN_OPERATIONS[i].equalsIgnoreCase(op)) return true ;
		return false ;
	}

	String          opType = null ;
	String          opId   = null ;
	String destinationPath = null ;

	String       startTime = null ;
	String         endTime = null ;
	long         startSeconds = -1 ;
	long           endSeconds = -1 ;

	long      transferSize = 0L ;
	int        resumeCount = 0 ;
	String         endMessage = null ;

	Properties requestDetails = null ;
	
	MirrorSpeeds nextElement = null ;
	
	public MirrorSpeeds(String operation, String destPath, String startDateTime, String opID) {
		if(operation == null) throw new NullPointerException("Operation string required (null)");
		if(!knownOperation(operation)) throw new IllegalArgumentException("Unknown operation: "+operation);
		
		if(destPath == null) throw new NullPointerException("Destination path required (null)");
		if(destPath.indexOf(':') < 0) throw new IllegalArgumentException("Destination path format should be SVM:volume, not: "+destPath);

		if(startDateTime == null) throw new NullPointerException("Starting time required (null)");
		if(hashNumbers(startDateTime).indexOf(HMS) < 0) throw new IllegalArgumentException("Start time should contain HH:mm:ss, not: "+startDateTime);
		
		if(opID == null) throw new NullPointerException("Operation ID required (null)");
		
		this.opType = operation ;
		this.destinationPath = destPath ;
		this.startTime = startDateTime ;
		this.startSeconds = convertToSeconds(startDateTime); 
		this.opId = opID ;
	}
	public int compareTo(MirrorSpeeds ms) {
		int cmp = this.destinationPath.compareTo(ms.destinationPath);
		if(cmp != 0) return cmp ;
		cmp = this.opType.compareTo(ms.opType);
		if(cmp != 0) return cmp ;
		cmp = (this.startSeconds > ms.startSeconds)?1:((this.startSeconds == ms.startSeconds)?0:1);
		if(cmp != 0) return (cmp < 0)?-1:1 ;
		return this.opId.compareTo(ms.opId);
	}
	public String toString() {
		StringBuilder buf = new StringBuilder();
		buf.append(this.destinationPath);
		buf.append(" ");
		buf.append(this.startTime);
		if(debugging) {
			buf.append('[');
			buf.append(this.startSeconds);
			buf.append(']');
		}
		if(this.endTime != null) {
			int delta = (int)(this.endSeconds - this.startSeconds);
			if(delta >= 0) {
				buf.append(" to ");
				buf.append(this.endTime);
				if(debugging) {
					buf.append('[');
					buf.append(this.endSeconds);
					buf.append(']');
			}	}
			if(this.transferSize < 0L) {
				buf.append(' ');
				buf.append(this.opType);
			} else {
				Vector<String> infoAtEnd = new Vector<String>();
				if(this.endMessage != null && this.endMessage.length() > 0) {
					StringTokenizer tokens = new StringTokenizer(this.endMessage, ";");
					while(tokens.hasMoreElements()) {
						String msg = tokens.nextToken();
						boolean found = false;
						for(int ht=0;!found && ht < HANDLED_TYPES;ht++)
							if(KNOWN_OPERATIONS[ht].equals(this.opType)) {
								found = true ;
								buf.append(SHOW_OPERATIONS[ht]);
								if(msg.startsWith(SUCCESS_MESSAGE)) {
									buf.append(" transferred ");
									buf.append(indent(""+this.transferSize, 11));
									buf.append(" bytes (");
									buf.append(indent(adjustedSize(this.transferSize), 8));
									buf.append(") in ");
									int tSecs = getTransferSeconds();
									buf.append(indent(secondsToTime(tSecs),11));
									if(debugging) {
										buf.append('[');
										buf.append(indent(""+tSecs, 6));
										buf.append(" seconds");
										buf.append(']');
									}
									buf.append(", transfer-speed=");
									buf.append(indent(transferSpeed(this.transferSize, tSecs),10));
									
									if(this.requestDetails != null) {
										buf.append(" REQ: ");
										buf.append(toInfo(this.requestDetails));
									}
								} else
								if(msg.startsWith(FAILURE_MESSAGE)) {
									buf.append(" FAILED");
									buf.append(msg.substring(FAILURE_MESSAGE.length()));
								} else
									infoAtEnd.addElement(msg);
							} else 
							if(!msg.equals("Success")) {
								buf.append(' ');
								buf.append(msg);
				}	}		}
				if(resumeCount > 0) {
					buf.append(" ("+resumeCount+" resumes)");
				}
				if(infoAtEnd.size() > 0) {
					for(int i=0;i<infoAtEnd.size();i++) {
						buf.append("\n\t");
						buf.append(infoAtEnd.elementAt(i));
		}	}	}	}
		return new String(buf);
	}
	
	void appendMessage(String msg) {
		if(this.endMessage == null) this.endMessage = msg ;
		else this.endMessage = this.endMessage + ";"+ msg ;
	}
	public void addInfoMessage(String message) {
		appendMessage(INFO_MESSAGE+message);
	}
	
	public void setFailureEnd(String endDateTime, String message) {
		if(endDateTime == null) throw new NullPointerException("Ending time required (null)");
		if(hashNumbers(endDateTime).indexOf(HMS) < 0) throw new IllegalArgumentException("Ending time should contain HH:mm:ss, not: "+endDateTime);

		this.endTime = endDateTime ;
		this.endSeconds = convertToSeconds(endDateTime);
		
		appendMessage((message != null)?FAILURE_MESSAGE+": "+message:FAILURE_MESSAGE);
	}
	public void setSuccessEnd(String endDateTime, long transferLength) {
		if(endDateTime == null) throw new NullPointerException("Ending time required (null)");
		if(hashNumbers(endDateTime).indexOf(HMS) < 0) throw new IllegalArgumentException("Ending time should contain HH:mm:ss, not: "+endDateTime);

		this.endTime = endDateTime ;
		this.endSeconds = convertToSeconds(endDateTime);
		
		this.transferSize = transferLength ;
		this.endMessage = SUCCESS_MESSAGE;
	}
	
	public void addEntry(MirrorSpeeds ms) {
		if(ms == this) throw new RuntimeException("Cannot add MirrorSpeed object to itself.");
		if(this.nextElement == null) this.nextElement = ms ;
		else 
		if(ms != this.nextElement) 
		{ // hang at the end
			MirrorSpeeds msElement = this.nextElement ;
			while(msElement.nextElement != null && ms != msElement.nextElement) msElement = msElement.nextElement ;
			msElement.nextElement = ms ; // was already if it hit the first condition :)
		}
	}
	public MirrorSpeeds nextEntry() {
		return this.nextElement ;
	}

	public String getOperation() {
		return this.opType ;
	}
	public String getDestination() {
		return this.destinationPath ;
	}
	public String getStartTime() {
		return this.startTime ;
	}
	public long getStartSeconds() {
		return this.startSeconds ;
	}

	public boolean hasEnded() {
		return this.endSeconds >= 0 && this.endMessage != null ;
	}
	public boolean wasSuccess() {
		return (this.endMessage == null)?false:this.endMessage.startsWith(SUCCESS_MESSAGE);
	}
	public String getFailureMessage() {
		return (this.endMessage != null && this.endMessage.startsWith(FAILURE_MESSAGE))?this.endMessage.substring(FAILURE_MESSAGE.length()+2):null ;
	}
	public String getEndTime() {
		return this.endTime ;
	}
	public long getEndSeconds() {
		return this.endSeconds ;
	}
	
	public long getTransferBytes() {
		return this.transferSize ;
	}
	public int  getTransferSeconds() {
		return (this.endSeconds > 0)?(int)(this.endSeconds-this.startSeconds):0;
	}
	
	void pickupRequestDetails(Hashtable<Long, Properties> requests) {
		if(requests != null) {
			Long searchSeconds = new Long(startSeconds);
			Properties details = requests.get(searchSeconds);
			while(details != null) {
				String dest = details.getProperty(REQ_TARGET);
				if(dest.equals(this.destinationPath)) {
					this.requestDetails = details ;
					details = null ;
				} else
					details = getNext(details);
	}	}	}
	
	static boolean debugging = false ;
	static boolean debugToError = false ;
	static void dbg(String message) {
		if(debugging) {
			if(debugToError)
				System.out.println("(Debug) "+message);
			else
				System.err.println("(Debug) "+message);
	}	}
	static void log(String message) {
		System.out.println(message);
	}
	static void warn(String message) {
		System.err.println("Warning: "+message);
	}
	static void err(String message) {
		System.err.println("ERROR: "+message);
	}
	static void err(String message, int exitCode) {
		err(message); System.exit(exitCode);
	}
	
	static String hashNumbers(String s) {
		StringBuilder buf = new StringBuilder();
		boolean changed = false ;
		for(int i=0;i<s.length();i++) {
			char ch = s.charAt(i);
			if(ch == '#') { ch = '?' ;  changed = true ; }
			else
			if(ch >= '0' && ch <= '9') { ch = '#' ;  changed = true ; }
			
			buf.append(ch);
		}
		return changed?new String(buf):s ;
	}
	
	// From ASUP: English only :)
	final static String [] WEEKDAYS = {
		"Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"
	};
	static int indexOfWeekday(String line) {
		int idx = -1 ;
		for(int i=WEEKDAYS.length-1;idx < 0 && i >= 0;i--)
			idx = line.indexOf(WEEKDAYS[i]);
		return idx ;
	}
	static int weekdayIndex(String wday) {
		if(wday.length() == 3) 
			for(int i=WEEKDAYS.length-1;i>=0;i--)
				if(wday.equalsIgnoreCase(WEEKDAYS[i])) return i ;
		return -1 ;
	}
	final static String [] MONTH_NAMES = {
		"Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"
	};
	static int monthIndex(String month) {
		if(month.length()  > 3) 
			month = month.substring(0,3); // cut long month short - just in case
		if(month.length() == 3) 
			for(int i=MONTH_NAMES.length-1;i>=0;i--)
				if(month.equalsIgnoreCase(MONTH_NAMES[i])) return i+1 ; // Jan = 1
		return -1 ;
	}
	final static int [] MONTH_LENGTHS = {
		31,28,31, 30,31,30, 31,31,30, 31,30,31
	};
	final static int  MIN_SECONDS = 60 ;
	final static int HOUR_SECONDS = 60 * MIN_SECONDS ;
	final static int  DAY_SECONDS = 24 *HOUR_SECONDS ;
	final static int YEAR_SECONDS = 365* DAY_SECONDS ; // without the Feb 29th days

	final static String TIME_FORMAT = "##:##:##" ;
	
	static long convertToSeconds(String dateTime) {
		int weekDay  = -1 ;
		int monthDay = 0 ;
		int month    = 0 ;
		int year     = 0 ;
		String timeS = null ;
		int seconds  = -1 ;
		StringBuilder restDump = new StringBuilder(); // timezone gets dumped here, and anything else not recognized
		StringTokenizer bits = new StringTokenizer(dateTime);
		while(bits.hasMoreTokens()) {
			String bit = bits.nextToken(); // expected order: weekday month day HH:mm:ss TZ YYYY  "Wed Jul  8 18:27:44 CEST 2020"
			
			// Normal strings expected
			if(weekDay < 0 && bit.length() == 3) {
				int idx = weekdayIndex(bit);
				if(idx >= 0) {
					weekDay = idx ;
					continue ; // cut the loop short
			}	}
			if(month == 0) {
				int idx = monthIndex(bit);
				if(idx > 0 && idx < 13) {
					month = idx ;
					continue ; // cut the loop short
			}	}

			// Numbers expected (or trash)
			String hash = hashNumbers(bit);
			if(monthDay == 0 && "##".equals(hash) || "#".equals(hash)) {
				try { 
					int idx = Integer.parseInt(bit);
					if(idx >= 1 && idx < 32) {
						monthDay = idx ;
						continue ; // cut the loop short
					}
				} catch(NumberFormatException nmf) { } // cannot happend, since it's either single or double digit from hashNumbers
			}
			if(seconds < 0 && TIME_FORMAT.equals(hash)) {
				try {
					int tHours = Integer.parseInt(bit.substring(0,2));
					int tMins  = Integer.parseInt(bit.substring(3,5));
					int tSecs  = Integer.parseInt(bit.substring(6,8));
					if(tHours < 24 && tMins < 60 && (tSecs < 60 || (tHours == 23 && tMins == 59 && tSecs == 60))) {
						timeS = bit ;
						seconds = tHours * 3600 + tMins * 60 + tSecs ;
						continue ;
					}
				} catch(NumberFormatException nmf) { } // cannot happen due to number check in hashNumbers and the format
			}
			if(year == 0 && "####".equals(hash)) {
				try {
					int idx = Integer.parseInt(bit);
					if(idx >= 2003 && idx < 2999) { // start of C-Mode ONTAP was with ONTAP 8.0, d.d. 
						year = idx ;
						continue ;
					}
				} catch(NumberFormatException nmf) { } // cannot happen due to number check in hashNumbers and the format of year
			}
			restDump.append(bit);
			restDump.append(' ');
		}
		if(year < 0 || month < 1 || monthDay < 1 || seconds < 0)
			throw new IllegalArgumentException("Date string does not contain all required parts [www MMM dd HH:mm:ss (TZ) YYYY]: "+dateTime);

		if(monthDay <= MONTH_LENGTHS[month-1] || ((year % 4) == 0 && month == 2 && monthDay == 29)) {
			// number of extra days of Feb 29th over the years, including current year (!)
			long totalSeconds = DAY_SECONDS * ((year - 1996)/4); 
			
			// seconds in days from years
			if((year-2000) > Integer.MAX_VALUE / YEAR_SECONDS)
				for(int y=0;y<year-2000;y++)
					totalSeconds += YEAR_SECONDS ;
			else
				totalSeconds += YEAR_SECONDS * (year-2000);
			
			// seconds in days from months since January
			for(int mIdx = 1; mIdx < month; mIdx++)
				totalSeconds += MONTH_LENGTHS[mIdx-1] * DAY_SECONDS ; // add the month days for each month
			if((year % 4) == 0 && month <= 2)
				totalSeconds -= DAY_SECONDS ; // subtract the four-yearly Feb 29th, since we have not past it yet
			
			// Days since the 1st
			totalSeconds += (monthDay-1) * DAY_SECONDS + seconds ;

			dbg("CONV2SECS: "+totalSeconds+" seconds from "+year+"/"+MONTH_NAMES[month-1]+"/"+((monthDay < 10)?"0":"")+monthDay+" "+timeS);
			return totalSeconds ;
		} else
			throw new IllegalArgumentException("Date string has an invalid day for month "+MONTH_NAMES[month-1]+": "+monthDay+" in: "+dateTime);
	}
	static String secondsToTime(int seconds) {
		StringBuilder buf = new StringBuilder();
		boolean indent = false ;
		if(seconds >= DAY_SECONDS) {
			int num = seconds/DAY_SECONDS;
			buf.append(num);
			buf.append('d');
			seconds -= num * DAY_SECONDS ;
			indent = true ;
		}
		if(seconds >= HOUR_SECONDS) {
			int num = seconds/HOUR_SECONDS;
			if(indent && num < 10) buf.append('0');
			buf.append(num);
			buf.append('h');
			seconds -= num * HOUR_SECONDS ;
			indent = true ;
		}
		if(seconds >= MIN_SECONDS) {
			int num = seconds/MIN_SECONDS;
			if(indent && num < 10) buf.append('0');
			buf.append(num);
			buf.append('m');
			seconds -= num * MIN_SECONDS ;
			indent = true ;
		}
		if(indent && seconds < 10) buf.append('0');
		buf.append(seconds);
		buf.append('s');
		return new String(buf);
	}
	static char [] KBITS = {
		' ', 'k', 'M', 'G', 'T'
	};
	static String adjustedSize(long size) {
		double sizeD = (double)size ;
		int kbit = 0 ;
		while(sizeD > 9999.0 && kbit < KBITS.length-1) { sizeD /= 1024.0 ; kbit++ ; }

		String numS = ""+sizeD ;
		int idx = numS.indexOf('.');
		return ((idx >= 0 && idx+2 <= numS.length())?numS.substring(0,idx+2):numS+".0")+KBITS[kbit]+"b" ;
	}
	static String transferSpeed(long size, int secs) {
		double speed = (double)size / (double)secs ;
		int kbit = 0 ;
		while(speed > 9999.0 && kbit < KBITS.length-1) { speed /= 1024.0 ; kbit++ ; }

		String numS = ""+speed ;
		int idx = numS.indexOf('.');
		return ((idx >= 0 && idx+2 <= numS.length())?numS.substring(0,idx+2):numS+".0")+KBITS[kbit]+"b/s" ;
	}
	
	static Properties propertyItems(String line) {
		line = line.trim();
		int idx = line.indexOf('=');
		if(idx > 0) {
			Properties prop = new Properties();
			while(idx > 0) {
				String name = line.substring(0,idx);
				int spIdx = line.indexOf(' ',idx+1);
				int eqIdx = line.indexOf('=',idx+1);
				if(eqIdx < 0) {
					prop.setProperty(name, line.substring(idx+1));
					line = "" ;
					idx = -1 ;
				} else {
					prop.setProperty(name, line.substring(idx+1, spIdx));
					line = line.substring(spIdx+1).trim();
					idx = eqIdx - spIdx - 1 ;
				}
			}
			return prop ;
		}
		return null ;
	}
	static String indent(String s, int len) {
		if(s.length() >= len) return s ;
		StringBuilder buf = new StringBuilder();
		for(int i=s.length();i<len;i++)
			buf.append(' ');
		buf.append(s);
		return new String(buf);
	}

	final static String MIRROR_OP_ID  = "Operation-Uuid";
	final static String MIRROR_ACTION = "action" ;
	final static String MIRROR_SOURCE = "source" ;
	final static String MIRROR_DEST   = "destination" ;
	final static String MIRROR_STATUS = "status" ;
	final static String MIRROR_MSG    = "message" ;
	final static String MIRROR_XFR    = "bytes_transferred" ;
	
	final static String ACTION_START  = "Start";
	final static String ACTION_END    = "End";
	final static String ACTION_RESUME = "Defer";
	final static String ACTION_INFO   = "Info";
	
	final static String RESUME_REQUIRED = "CSM: An operation did not complete within the specified timeout window.";
	
	static boolean wincmd = File.separatorChar == '\\' ;
	static boolean isOption(String parm) {
		return parm.startsWith("-") || (wincmd && parm.startsWith("/"));
	}
	static String getOption(String parm) {
		if(isOption(parm)) {
			String retval = parm.substring(1);
			if(retval.length() > 0 && parm.startsWith("--")) retval = parm.substring(2);
			if(retval.length() > 0) return wincmd?retval.toLowerCase():retval ;
		}
		return null ;
	}
	static int indexOfOption(String option) {
		for(int i=0;i < OPTIONS.length;i+=3)
			if(OPTIONS[i].equals(option) || OPTIONS[i+1].equals(i)) return i / 3 ;
		return -1 ;
	}
	static void printOptions() {
		log("OPTIONS:");
		for(int i=0;i < OPTIONS.length;i+=3) {
			String opt    = wincmd?"/"+OPTIONS[  i].toUpperCase():"-" +OPTIONS[i] ;
			String option = wincmd?"/"+OPTIONS[i+1].toUpperCase():"--"+OPTIONS[i+1] ;
			log("\t"+opt+"|"+option+"\t"+OPTIONS[i+2]);
		}
	}
	static void printUsage()  {
		log(USAGE);
		printOptions();
		log(VERSION_HISTORY);
	}
	
	public static void main(String [] args) {
		log(MIRRORSPEEDS_INFO);
		
		Hashtable<Long,Properties> infoIndex = null ;
		Vector<File> logFiles = new Vector<File>();
		boolean skipFailures = false ;
		
		if(args != null)
			for(int i=0;i<args.length;i++)
				if(args[i] != null) {
					String option = getOption(args[i]); // check if it's an option, if not => null
					
					if(option != null) {
						int optIdx = indexOfOption(option);
						if(optIdx < 0) {
							printOptions();
							err("Unknown option: "+args[i], 5);
						}
						switch(optIdx) {
							case 0:
								debugging = true ;
								debugToError = true ;
								break ;
							case 1:
								debugging = true ;
								break ;
							case 2:
								printUsage();
								System.exit(0);
								break ;
							case 3:
								skipFailures = true ;
								break ;
							case 4:
								if(i+1 < args.length) {
									String mlogFile = args[++i];
									File fl = new File(mlogFile);
									if(!fl.exists() || !fl.isFile())
										err("Given (XML) audit log file does not exist:\nFILE:\t"+fl.getAbsolutePath(), 50);
									else {
										if(infoIndex == null) infoIndex = new Hashtable<Long,Properties>();
										int found = infoFromAuditMLog(fl, infoIndex);
										log("Found "+found+" snapmirror-update requests in this Audit-MLog.");
								}	}
						}
					} else {
						File fobj = new File(args[i]);
						if(!fobj.exists())
							err("Given file system object does not exist.\nFOBJ:\t"+fobj.getAbsolutePath(), 5);
						else
						if(!fobj.isFile())
							err("Cannot use directory as parmameter - give a log file.\nDIR:\t"+fobj.getAbsolutePath());
						else
							logFiles.addElement(fobj);
				}	}
		if(logFiles.size() == 0)
			err("Program requires at least one ONTAP SnapMirror Audit-log file as a parameter.");

		Hashtable<String,MirrorSpeeds> destinationSpeeds = new Hashtable<String,MirrorSpeeds>(); // from destination
		Hashtable<String,MirrorSpeeds>   operationSpeeds = new Hashtable<String,MirrorSpeeds>(); // from operation id
		
		for(int lf=0;lf < logFiles.size();lf++) {
			File logFile = logFiles.elementAt(lf);
			BufferedReader bread = null ;
			try {
				bread = new BufferedReader(new FileReader(logFile));
				String line = null ;
				while((line = bread.readLine()) != null) {
					int idx = line.indexOf('['); // the operation name is just before
					if(idx > 0) {
						int preIdx = idx ;
						while(preIdx > 0 && line.charAt(preIdx-1) != ' ') preIdx-- ;
						if(preIdx > 0 && preIdx < idx-1) {
							String   opCmd  = line.substring(preIdx, idx);				/** COMMAND **/
							String dateTime = line.substring(0,preIdx).trim();			/** DATETIME ***/
							
							idx = line.indexOf("]:");
							if(idx > 0) {
								while(idx < line.length() && line.charAt(idx) != ' ') idx++ ;
								if(idx < line.length()) {
									Properties items = propertyItems(line.substring(idx));
									if(items != null) {
										String opId = items.getProperty(MIRROR_OP_ID);	/** OPERATION-ID **/
										if(opId != null && opId.length() > 0) {
											MirrorSpeeds existingOperation = operationSpeeds.get(opId);
											String action = items.getProperty(MIRROR_ACTION); /** ACTION **/
	
											if(ACTION_START.equals(action)) {
												String destPath = items.getProperty(MIRROR_DEST); /** DESTINATION **/
												if(existingOperation == null && destPath != null)
													try {
														MirrorSpeeds newOperation = new MirrorSpeeds(opCmd, destPath, dateTime, opId);
														operationSpeeds.put(opId, newOperation);
														
														existingOperation = destinationSpeeds.get(destPath);
														if(existingOperation == null) 
															destinationSpeeds.put(destPath, newOperation);
														else
															existingOperation.addEntry(newOperation); // hang in the queue
													} catch(Exception x) {
														err("Unable to create operation for '"+opCmd+"' Start action at "+dateTime+"\nEXCEPTION: "+x.toString());
													}
											} else
											if(ACTION_RESUME.equals(action)) {
												if(existingOperation != null && line.indexOf(RESUME_REQUIRED) > 0) {
													existingOperation.resumeCount++ ;
												}
											} else
											if(ACTION_END.equals(action)) {
												if(existingOperation != null && !existingOperation.hasEnded()) {
													String status = items.getProperty(MIRROR_STATUS);
													if(SUCCESS_MESSAGE.equalsIgnoreCase(status)) {
														String transfer = items.getProperty(MIRROR_XFR);
														if(transfer != null && transfer.length() > 0) {
															long xfrLen = -1L ;
															try {
																xfrLen = Long.parseLong(transfer);
															} catch(NumberFormatException nmf) {
																err("Unable to set operation '"+existingOperation.getOperation()+"' End action to Success at "+dateTime+"\nEXCEPTION: transfer length is not a number: "+transfer);
															}
															if(xfrLen >= 0L)
																try { existingOperation.setSuccessEnd(dateTime, xfrLen); }
																catch(Exception x) { err("Unable to set operation '"+existingOperation.getOperation()+"' End action to Success at "+dateTime+"\nEXCEPTION: "+x.toString()); }
														} else {
															try { existingOperation.setSuccessEnd(dateTime, -1L); }
															catch(Exception x) { err("Unable to set operation '"+existingOperation.getOperation()+"' End action to Success at "+dateTime+"\nEXCEPTION: "+x.toString()); }
														}
													} else
													if(FAILURE_MESSAGE.equalsIgnoreCase(status)) {
														String msg = items.getProperty(MIRROR_MSG);
														try { existingOperation.setFailureEnd(dateTime, msg); }
														catch(Exception x) { err("Unable to set operation '"+existingOperation.getOperation()+"' End action to Failure at "+dateTime+"\nEXCEPTION: "+x.toString()+"\nMSG:\t"+msg); }
													}
												}
											} else
											if(action.startsWith(ACTION_INFO)) {
												if(existingOperation != null) {
													String msg = action.substring(ACTION_INFO.length()).trim();
													if(msg.length() > 0) existingOperation.addInfoMessage(msg);
							}	}	}	}	}	}
					}	}
				}
				bread.close();
				log("Total operations: "+operationSpeeds.size()+" after loading "+logFile.toString());
			} catch(IOException iox) {
				err("I/O Error "+iox.toString()+" reading:\nFILE:\t"+logFile.getAbsolutePath());
				if(bread != null) try { bread.close(); } catch(IOException closingException) { }
			}
		}
		
		// Now we can output all the success transfer data and possibly to stats analysis on repeated transfer attempts
		if(destinationSpeeds.size() > 0) {
			Vector<String> destinations = new Vector<String>();
			Enumeration<String> destNames = destinationSpeeds.keys();
			while(destNames.hasMoreElements()) {
				String dest = destNames.nextElement();
				
				// sort into Vector
				int index = destinations.size();
				while(index > 0 && destinations.elementAt(index-1).compareTo(dest) > 0) index-- ;
				destinations.insertElementAt(dest, index);
			}
			
			for(int dIdx = 0;dIdx < destinations.size();dIdx ++) {
				String dest = destinations.elementAt(dIdx);
				log("Destination: "+dest);
				
				MirrorSpeeds ms = destinationSpeeds.get(dest);
				while(ms != null) {
					if(infoIndex != null) ms.pickupRequestDetails(infoIndex);
				
					if(!skipFailures || ms.wasSuccess())
						log("\t"+ms.toString().substring(dest.length()));
					ms = ms.nextEntry();
				}
			}
		}
		
	}
	
	final static int DOUBLE_COLON_CAPACITY = 6 ;
	final static String SNAP_UPDATE     = "snapmirror-update" ; // only pick the requests, not the results
	final static String SNAP_UPDATE_XML = "<"+SNAP_UPDATE+">" ; // only pick the requests, not the results
	
	final static String REQ_DATETIME   = "Date-Time" ;
	final static String REQ_TCP_SOURCE = "Host-IP" ;
	final static String REQ_VSERVER    = "VServer" ;
	final static String REQ_ACCOUNT    = "Account" ;

	final static String REQ_SOURCE    = "Source" ;
	final static String REQ_TARGET    = "Destination" ;
	final static String REQ_SNAPSHOT  = "Snapshot" ;
	
	final static String REQ_NEXT      = "Next-Match" ;
	
	final static String [] REQUEST_ITEMS = {
		REQ_DATETIME, REQ_TCP_SOURCE,
		REQ_VSERVER , REQ_ACCOUNT,
		REQ_SOURCE,   REQ_TARGET,
		REQ_SNAPSHOT
	};
	final static String [] XML_PARMS = {
		null,null,null,null,
		"source-location",
		"destination-location",
		"source-snapshot"
	};
	
	static Hashtable <Integer, Properties> additionalIndex = new Hashtable <Integer, Properties>();
	static Properties getNext(Properties previous) {
		String hash = previous.getProperty(REQ_NEXT);
		if(hash != null && hash.length() > 0) 
			try {
				Integer searchInt = new Integer(hash);
				return additionalIndex.get(searchInt);
			} catch(NumberFormatException nmf)  { }
		return null ;
	}
	static void setNext(Properties previous, Properties next) {
		int hash = next.hashCode();
		additionalIndex.put(new Integer(hash), next);
		previous.setProperty(REQ_NEXT, ""+hash);
	}
	static String toInfo(Properties details) {
		String snap = details.getProperty(REQ_SNAPSHOT);
		if(snap == null) return null ;

		StringBuilder buf = new StringBuilder();
		buf.append("Snapshot=");
		buf.append(snap);
		String host = details.getProperty(REQ_TCP_SOURCE);
		if(host != null) {
			buf.append(", Host=");
			buf.append(host);
		}
		String account = details.getProperty(REQ_ACCOUNT);
		if(account != null) {
			buf.append("SVM-user=");
			buf.append(account);
		}
		return new String(buf);
	}
	
	static int infoFromAuditMLog(File fl, Hashtable<Long,Properties> store) {
		if(fl    == null) throw new NullPointerException("Audit file required (null).");
		if(store == null) throw new NullPointerException("Store for hash required (null).");
		
		BufferedReader bread = null ;
		int count = 0 ;
		try {
			bread = new BufferedReader(new FileReader(fl));
			log("Scanning Audit-Mlog (XML) to store request info: "+fl.toString());
			String line = null ;
			while((line = bread.readLine()) != null) {
				if(line.indexOf(SNAP_UPDATE_XML) > 0) {
					String [] parts = separate(line, " :: ");
					if(parts != null && parts.length >= 5) {
						// Date/Time
						int i1 = indexOfWeekday(parts[0]);
						int i2 = parts[0].indexOf('[');
						if(i1 >= 0 && i2 >= 0 && i1 < i2) try {
							String dateTime = parts[0].substring(i1, i2).trim();
							long seconds = convertToSeconds(dateTime);
							
							Properties p = new Properties();
							p.setProperty(REQ_DATETIME, dateTime);
							i1 = parts[2].indexOf(':');
							p.setProperty(REQ_TCP_SOURCE, (i1 > 0)?parts[2].substring(0,i1):parts[2]); // IP:Port
							i1 = parts[3].indexOf(':');
							if(i1 > 0) {
								p.setProperty(REQ_VSERVER,  parts[3].substring(0,i1)); // VServer
								p.setProperty(REQ_ACCOUNT, parts[3].substring(i1+1)); // Account
							}
							String xml = getXMLValue(parts[4], SNAP_UPDATE);
							if(xml != null && xml.length() > 0) {
								for(int i=XML_PARMS.length-1;i>=0;i--)
									if(XML_PARMS[i] != null) {
										String value = getXMLValue(xml, XML_PARMS[i]);
										if(value != null) p.setProperty(REQUEST_ITEMS[i], value);
									}
								Properties old = store.put(new Long(seconds), p);
								if(old != null) setNext(p, old); // insert into queue :)
								count++ ;
							}
						} catch(Exception x) {
							x.printStackTrace();
						}
			}	}	}
			bread.close();
		} catch(IOException iox) {
			if(bread != null) try { bread.close(); } catch(IOException breadX) { }
			err("I/O Error reading from audit-mlog: "+iox.toString()+"\nFILE:\t"+fl.getAbsolutePath());
		}
		return count ;
	}
	static String getXMLValue(String xml, String name) {
		String foo = "<" +name+">" ;
		String bar = "</"+name+">" ;
		int i1 = xml.indexOf(foo);
		int i2 = xml.indexOf(bar);
		if(i1 > 0 && i2 > 0 && i1 < i2) 
			return xml.substring(i1+foo.length(), i2);
		return null ;
	}
	
	static Vector<String> items = new Vector<String>(DOUBLE_COLON_CAPACITY);
	static String [] separate(String str, String sep) {
		synchronized(items) {
			int idx = -1, len = sep.length();
			items.removeAllElements();
			
			// first bits
			while((idx = str.indexOf(sep)) >= 0) {
				String item = str.substring(0,idx).trim();
				str = str.substring(idx+len);
				if(item.length() > 0) items.addElement(item);
			}
			// last bit
			str = str.trim();
			if(str.length() > 0) items.addElement(str);
			
			if(items.size() > 0) {
				String [] itemStr = new String [items.size()];
				return items.toArray(itemStr);
			}
			return null ;
	}	}
}