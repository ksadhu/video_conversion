//This program converts the given video file(in any format) into the target format(avi,mp4,mpg,mpeg) on hadoop framework
//input: a text file containing the path of the video file on HDFS followed by a space and then the target format(in each line; one line for each video)
//output: a text file containing the path(on HDFS) of the converted video file(s) (one file path on each line)
//arguments: <input directory on HDFS><space><output directory on HDFS>

package iiit.ac.in;
import java.io.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.io.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PVidConvert {

	    public static class Map1 extends Mapper<Object, Text, Text, Text>{
	    		// The input video files are split into chunks of 64MB here...
		        public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
		        		String line = value.toString();
		        		System.out.println("job1:mapInp:-"+line);
		        		String[] info=line.split(" ");
		        		info[0]=info[0].trim();
		        		info[1]=info[1].trim();
		        		String lstfnames="",fname="";
		        		try {
		        			Configuration config = new Configuration();
		    				FileSystem hdfs = FileSystem.get(config);
		    				String prefixPath="",fnm="";
		    				Pattern x= Pattern.compile("(.*)/(.*)");
		        			Matcher xm=x.matcher(info[0]);
		        			while(xm.find()){
		        				prefixPath=xm.group(1);
		        				fnm=xm.group(2);
		        			}
		    				String dst="/home/"+fnm; 										//dst is path of the file on local system.
		    				hdfs.copyToLocalFile(new Path(info[0]), new Path(dst));			
		    				
		        			
		        			Process p = Runtime.getRuntime().exec("ffmpeg -i "+dst);
		        			String s;
		        	
		        			BufferedReader stdError = new BufferedReader(new InputStreamReader(p.getErrorStream()));
		        			Pattern D= Pattern.compile("Duration:[ ]*([0-9]+):([0-9]+):([0-9]+)");
		        			long time=0;							//"time" is the duration of the input video file
		        			long sps=0;								//"sps" is the number of seconds(duration) of each video split
		        			while ((s = stdError.readLine()) != null) {
		        				Matcher md=D.matcher(s);
		        				while(md.find()){	
		        					time=Long.parseLong(md.group(1))*3600+Long.parseLong(md.group(2))*60+Long.parseLong(md.group(3));
		        				}
		        			}
		        			Process p1 = Runtime.getRuntime().exec("du -s "+dst);
		        			BufferedReader stdInput1 = new BufferedReader(new InputStreamReader(p1.getInputStream()));
		        			String s1="",size="";					//"size" is the size of input video file
		        			while((s1 = stdInput1.readLine()) != null){
		        				String s11[]=s1.split("\t");
		        				size=s11[0];
		        			}
		        			sps=(64*1024)*time/(Long.parseLong(size));				// chunk size is 64MB
		        			String hr,min,sc;
		        			hr=Long.toString((sps/3600));
		        			min=Long.toString((sps%3600)/60);
		        			sc=Long.toString(sps%60);
		        			if(hr.length()<2) hr="0"+hr;
		        			if(min.length()<2) min="0"+min;
		        			if(sc.length()<2) sc="0"+sc;
		        			String splt=hr+":"+min+":"+sc;
		        			
		        			String query="mencoder -oac copy -ovc copy -ss ";		//building query to split the input video file
		        			String app="",inpExt="";
		        			Pattern xx= Pattern.compile("(.*)\\.(.*)");
		        			Matcher xxm=xx.matcher(dst);
		        			while(xxm.find()){
		        				fname=xxm.group(1);
		        				inpExt=xxm.group(2);
		        			}
		        			String[] tmpArr=fname.split("/");
		        			String hdfsFname="";
		        			long stSrt=0;
		        			int cnt=0;
		        			
		        			while(true){
		        				if(stSrt>time) break;
		        				if(stSrt+sps>time){
		        					long t=time-stSrt;
		        					hr=Long.toString((t/3600));
		        					min=Long.toString((t%3600)/60);
		        					sc=Long.toString(t%60);
		        					if(hr.length()<2) hr="0"+hr;
		        					if(min.length()<2) min="0"+min;
		        					if(sc.length()<2) sc="0"+sc;
		        					splt=hr+":"+min+":"+sc;
		        				}
		        				cnt++;
		        				hr=Long.toString((stSrt/3600));
		        				min=Long.toString((stSrt%3600)/60);
		        				sc=Long.toString(stSrt%60);
		        				if(hr.length()<2) hr="0"+hr;
		        				if(min.length()<2) min="0"+min;
		        				if(sc.length()<2) sc="0"+sc;
		        				app=hr+":"+min+":"+sc+" -endPos "+splt+" "+dst+" -o "+fname+"_"+Integer.toString(cnt)+"."+inpExt;
		        				
		        				Process p2 = Runtime.getRuntime().exec(query+app);
		        				String ls_str="";
		        				DataInputStream ls_in = new DataInputStream(p2.getInputStream());
		        				while ((ls_str = ls_in.readLine()) != null)
		        				{}
		        				p2.destroy();
		        				String[] tmpArr1=fnm.split("\\.");
		        				hdfs.copyFromLocalFile(true, true, new Path(fname+"_"+Integer.toString(cnt)+"."+inpExt),new Path(prefixPath+"/"+tmpArr1[0]+"_"+Integer.toString(cnt)+"."+inpExt));
		        				lstfnames+=prefixPath+"/"+tmpArr1[0]+"_"+Integer.toString(cnt)+"."+inpExt+" #!# ";
		        				stSrt+=sps;
		        			}
		        			Runtime rt1 = Runtime.getRuntime();
							String[] cmd1 = {"/bin/bash","-c","rm "+dst};			//delete the file after use
							Process pr1 = rt1.exec(cmd1);
							pr1.waitFor();
		        			lstfnames+="*"+info[1];
		        			
		        			context.write(new Text(fname),new Text(lstfnames));		//"fname" contains name of the input video file with extension(eg.".avi") removed #### "lstfnames" is a string, contains all the names of video splits(concatenated)
		        			System.out.println("lstfnames : "+lstfnames);		        			
		        		}
		        		catch (IOException e) {
		        			System.out.println("exception happened - here's what I know: ");
		        			e.printStackTrace();
		        			System.exit(-1);
		        		}
	        }
		}
	    public static class Reduce1 extends Reducer<Text, Text, Text, Text>{
	    		//Identity
		        public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException{
		        	System.out.println("I'm in Job1 reduce");
	                for(Text val: values){
	                	System.out.println("job1:redInp:-"+val.toString());
	                	context.write(new Text(""),val);
	                }
		        }
		    }
	    
	    public static class Map2 extends Mapper<Object, Text, Text, Text>{
	    	//Video splts are converted to target format here...
	    	public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
	    	try {
	    		Configuration config = new Configuration();
				FileSystem hdfs = FileSystem.get(config);
				
				String st=value.toString();
				st=st.trim();
				System.out.println("job2:mapInp:-"+st);
	    		String[] fmt=st.split(" #!# \\*");
				String[] lst=fmt[0].split(" #!# ");
				
				String out="",dlt="";
				int flag=1;
				for(String st1:lst){
        			
        			Pattern x= Pattern.compile("(.*)/(.*)");
        			Matcher xm=x.matcher(st1);
        			String prefixPath="",fnm="",inpExt="";
        			while(xm.find()){
        				prefixPath=xm.group(1);
        				fnm=xm.group(2);
        			}
        			String[] tmpArr=fnm.split("\\.");
        			fnm=tmpArr[0];
        			inpExt=tmpArr[1];
        			hdfs.copyToLocalFile(true, new Path(st1), new Path("/home/"+fnm+"."+inpExt));
        			String fname="/home/"+fnm;
					if(flag==1) {
						flag=0;
						out+=prefixPath+"/"+fnm+"."+fmt[1];
					}
					else{
						out+=" #!# "+prefixPath+"/"+fnm+"."+fmt[1];
					}
					
					if(fmt[1].equals("mpg") || fmt[1].equals("mpeg") || fmt[1].equals("mp4")){
						
						Process p = Runtime.getRuntime().exec("mencoder -of mpeg -ovc lavc -lavcopts vcodec=mpeg1video -oac copy "+"/home/"+fnm+"."+inpExt+" -o "+fname+"."+fmt[1]);
						
						String ls_str="";
						DataInputStream ls_in = new DataInputStream(p.getInputStream());
						while ((ls_str = ls_in.readLine()) != null){
							
						}
						
						p.destroy();
						dlt+=" /home/"+fnm+"."+inpExt;
					}
					else if(fmt[1].equals("avi")){
						
						Process p = Runtime.getRuntime().exec("mencoder -ovc lavc -oac mp3lame -o "+fname+"."+fmt[1]+" "+"/home/"+fnm+"."+inpExt);
						
						String ls_str="";
						DataInputStream ls_in = new DataInputStream(p.getInputStream());
						while ((ls_str = ls_in.readLine()) != null){
				
						}
						p.destroy();
						dlt+=" /home/"+fnm+"."+inpExt;
					}
					else{
						//TBD
						System.out.println("Unsupported target format!!!!!");
					}
					hdfs.copyFromLocalFile(true, true, new Path(fname+"."+fmt[1]), new Path(prefixPath+"/"+fnm+"."+fmt[1]));
				}
				
				Runtime rt1 = Runtime.getRuntime();
				String[] cmd1 = {"/bin/bash","-c","rm"+dlt};			//delete the files after use
				Process pr1 = rt1.exec(cmd1);
				pr1.waitFor();
				
				System.out.println("Job2 mapOut:"+out);
				context.write(new Text(lst[0]),new Text(out));
				System.out.println(out);
	    	}
	    	catch (IOException e) {
    			System.out.println("exception happened - here's what I know: ");
    			e.printStackTrace();
    			System.exit(-1);
    		}
	    }
	    	
	   }
	    public static class Reduce2 extends Reducer<Text, Text, Text, Text>{
	    	// merge the converted files here
	        public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException{
	        	System.out.println("I'm in Job2 reduce");
	        	
	        	Configuration config = new Configuration();
				FileSystem hdfs = FileSystem.get(config);
	        	try{
	        	String out="";
	        	for(Text t: values){
	        		out=t.toString();
	        		out=out.trim();
	        		System.out.println("job2:redInp:-"+out);
	        		break;
	        	}
	        	String[] outl = out.split(" #!# ");
				
	        	Pattern x= Pattern.compile("(.*)/(.*)\\.(.*)");
    			Matcher xm=x.matcher(outl[0]);
    			String prefixPath="",fnm="",ext="";
    			while(xm.find()){
    				prefixPath=xm.group(1);
    				fnm=xm.group(2);
    				ext=xm.group(3);
    			}
    			String foutname=fnm.split("_")[0];
    			foutname+="."+ext;
				String query="mencoder -oac copy -ovc copy";
				int cnt=0;
				for(String st:outl){
					cnt++;
					hdfs.copyToLocalFile(true, new Path(st), new Path("/home/"+fnm.split("_")[0]+"_"+Integer.toString(cnt)+"."+ext) );
					query+=" "+"/home/"+fnm.split("_")[0]+"_"+Integer.toString(cnt)+"."+ext;
				}
				query+=" -o "+"/home/"+foutname;
				Process p2 = Runtime.getRuntime().exec(query);			//query for merging the video files is executed here
				String ls_str="";
				DataInputStream ls_in = new DataInputStream(p2.getInputStream());
				while ((ls_str = ls_in.readLine()) != null){
				}
				p2.destroy();
				hdfs.copyFromLocalFile(true, true, new Path("/home/"+foutname), new Path(prefixPath+"/"+foutname));
				cnt=0;
				String dlt1="";
				for(String st3: outl){
					cnt++;
					dlt1+=" "+"/home/"+fnm.split("_")[0]+"_"+Integer.toString(cnt)+"."+ext;
				}
				Runtime rt1 = Runtime.getRuntime();
				String[] cmd1 = {"/bin/bash","-c","rm"+dlt1};			//delete the files after use
				Process pr1 = rt1.exec(cmd1);
				pr1.waitFor();
				context.write(new Text(""),new Text(prefixPath+"/"+foutname));
			}
	        	catch (IOException e) {
	    			System.out.println("exception happened - here's what I know: ");
	    			e.printStackTrace();
	    			System.exit(-1);
	    		}
	        }
	    }
    
	    
	    public static void main(String[] args) throws IOException,ClassNotFoundException,InterruptedException{
	        	
	    	Configuration conf = new Configuration();
		    Job job = new Job(conf,"job");
		    
		    job.setJarByClass(PVidConvert.class);   
		    job.setMapperClass(Map1.class);
		    job.setReducerClass(Reduce1.class);
		    job.setOutputKeyClass(Text.class);
		    job.setOutputValueClass(Text.class);
		    FileInputFormat.addInputPath(job, new Path(args[0]));
		    FileOutputFormat.setOutputPath(job, new Path("/tmp/temporary_execution/"));
		    job.waitForCompletion(true);
		    
		    
		    Configuration conf1 = new Configuration();
		    Job job1 = new Job(conf1,"job1");
		    
		    job1.setJarByClass(PVidConvert.class);   
		    job1.setMapperClass(Map2.class);
		    job1.setReducerClass(Reduce2.class);
		    job1.setOutputKeyClass(Text.class);
		    job1.setOutputValueClass(Text.class);
		    FileInputFormat.addInputPath(job1, new Path("/tmp/temporary_execution/"));
		    FileOutputFormat.setOutputPath(job1, new Path(args[1]));
		    job1.waitForCompletion(true); 
	    	}
}
