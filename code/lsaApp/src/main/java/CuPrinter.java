package com.lsa.app;

import java.util.zip.ZipInputStream;
import com.github.javaparser.JavaParser;
import com.github.javaparser.ast.CompilationUnit;
import java.io.IOException;
import java.io.FileInputStream;
import java.io.StringReader;
import java.io.Reader;
import java.lang.Throwable;


public class CuPrinter {
    public static void main(String[] args) throws Exception {
        // creates an input stream for the file to be parsed
        FileInputStream in = new FileInputStream("JavaCommentsRemover.java");
	
        CompilationUnit cu;
        try {
            // parse the file
            cu = JavaParser.parse(in);
        } finally {
       		in.close();
        }

        // prints the resulting compilation unit to default system output
        System.out.println(cu.toString());
    }
}
