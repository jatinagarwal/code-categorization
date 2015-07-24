/* 
 * Copyright 2014 Igor Maznitsa (http://www.igormaznitsa.com).
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
//package com.igormaznitsa.jcp.removers;

//import com.igormaznitsa.jcp.utils.PreprocessorUtils;
package com.lsa.app;

import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import java.io.StringReader;
import java.io.StringWriter;

/**
 * A remover allows to cut off all Java like comments from a reader and write
 * the result into a writer
 *
 * @author Igor Maznitsa (igor.maznitsa@igormaznitsa.com)
 */
public class JavaCommentsRemover {

  private final Reader srcReader;
  private final Writer dstWriter;

  public JavaCommentsRemover(final Reader src, final Writer dst) {
    //PreprocessorUtils.assertNotNull("The reader is null", src);
    //PreprocessorUtils.assertNotNull("The writer is null", dst);
    this.srcReader = src;
    this.dstWriter = dst;
  }

  void skipUntilNextString() throws IOException {
    while (true) {
      final int chr = srcReader.read();
      if (chr < 0) {
        return;
      }

      if (chr == '\n') {
        dstWriter.write(chr);
        return;
      }
    }
  }

  void skipUntilClosingComments() throws IOException {
    boolean starFound = false;

    while (true) {
      final int chr = srcReader.read();
      if (chr < 0) {
        return;
      }
      if (starFound) {
        if (chr == '/') {
          return;
        }
        else {
          starFound = chr == '*';
        }
      }
      else {
        if (chr == '*') {
          starFound = true;
        }
      }
    }
  }

  public String process() throws IOException {
    final int STATE_NORMAL = 0;
    final int STATE_INSIDE_STRING = 1;
    final int STATE_NEXT_SPECIAL_CHAR = 2;
    final int STATE_FORWARD_SLASH = 3;

    int state = STATE_NORMAL;

    // System.out.println("Inside process method");

    while (true) {
      final int chr = srcReader.read();
      if (chr < 0) {
        // System.out.println("character is less than 0");
        // System.out.println(chr);
        break;
      }

      switch (state) {
        case STATE_NORMAL: {
          switch (chr) {
            case '\"': {
              dstWriter.write(chr);
              // System.out.print((char)chr);
              state = STATE_INSIDE_STRING;
            }
            break;
            case '/': {
              state = STATE_FORWARD_SLASH;
            }
            break;
            default: {
              dstWriter.write(chr);
              // System.out.print((char)chr);
            }
            break;
          }
        }
        break;
        case STATE_FORWARD_SLASH: {
          switch (chr) {
            case '*': {
              skipUntilClosingComments();
              state = STATE_NORMAL;
            }
            break;
            case '/': {
              skipUntilNextString();
              state = STATE_NORMAL;
            }
            break;
            default: {
              dstWriter.write('/');
              dstWriter.write(chr);
              // System.out.print((char)chr);
              state = STATE_NORMAL;
            }
            break;
          }
        }
        break;
        case STATE_INSIDE_STRING: {
          switch (chr) {
            case '\\': {
              state = STATE_NEXT_SPECIAL_CHAR;
            }
            break;
            case '\"': {
              state = STATE_NORMAL;
            }
            break;
          }
          dstWriter.write(chr);
          // System.out.print((char)chr);
        }
        break;
        case STATE_NEXT_SPECIAL_CHAR: {
          dstWriter.write(chr);
          // System.out.print((char)chr);
          state = STATE_INSIDE_STRING;
        }
        break;
      }
      dstWriter.flush();
    }
    return dstWriter.toString();
  }

  // public String removeComments(String str) {
  //   String withoutComments = "";
  //   try {
  //     Reader reader = new StringReader(str);
  //     System.out.println("from reader: "+reader.toString());
  //     StringWriter writer = new StringWriter(1024000);  
  //     JavaCommentsRemover jcr = new JavaCommentsRemover(reader,writer);
  //     jcr.process();
  //     withoutComments = writer.toString();
  //     System.out.println("from writer: "+withoutComments);      
  //   }
  //   catch(IOException e) { 
  //     e.printStackTrace();
  //     System.out.println("An error occured while trying to write to the file");
  //   }  
  //   return withoutComments;
  // }

  // public static void main(String[] args) {
  //     String st = "jatin //first name"+"\n"+" agarwal/*last name*/";
  //     JavaCommentsRemover jcr = new JavaCommentsRemover();
  //     String res = jcr.removeComments(st);
  //     System.out.println("Result: "+res);
  //     // Reader reader = new StringReader(st);
  //     // StringWriter writer = new StringWriter(1024000);
  //     // System.out.println("from reader: "+reader.toString());
  //     // JavaCommentsRemover jcr = new JavaCommentsRemover(reader,writer);
  //     // jcr.process();
  //     // System.out.println("from writer: "+writer.toString());    
  // }

}