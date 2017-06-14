package com.boundlessgeo;

import java.io.File;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class DoittprocessorApplication implements CommandLineRunner {
	private final Logger logger = LoggerFactory.getLogger(this.getClass());
	@Autowired
	ProcessShapefile psf;

	public static void main(String[] args) {
		SpringApplication.run(DoittprocessorApplication.class, args);
	}
	
	@Override
	public void run(String... arg0) throws Exception {
		if(arg0.length==1){
			if(arg0[0].contains("zip")){
				String shp = arg0[0].substring(arg0[0].indexOf("=")+1);
				logger.info(shp);
				File zipfile = new File(shp);
				File fieldssplitShape = psf.processZipShape(zipfile,"bikepathtemp","bikepath.shp",true,false);
				logger.info(fieldssplitShape.getAbsolutePath());
			}else{
				logger.warn("Path does not point at file with zip extension");
			}
			
		}else{
			logger.warn("Too many arguments.  Should just be path to zip of shapefile");
		}
		
	}
}
