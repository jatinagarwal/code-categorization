/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.lsa.app

import com.lsa.app.CuPrinter

import java.io.{ByteArrayOutputStream, File}
import java.util.zip.{ZipEntry, ZipInputStream}
import java.io.Reader;
import java.io.IOException;
import java.io.StringReader;
import java.lang.Throwable;

import com.github.javaparser.JavaParser
import com.github.javaparser.ast.CompilationUnit

import org.apache.commons.io.IOUtils
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.StringBuilder
import scala.util.Try
import scala.util.control.ControlThrowable

/**
 * Extracts and merges java files from the given zip file. Also removes comment from java files
 */

 trait Logger {
  val log = LoggerFactory.getLogger(this.getClass.getName)
}


object ZipBasicParser extends Logger {

  private val bufferSize = 1024000 // about 1 mb

  def readFilesAndPackages(zipStream: ZipInputStream) : (String,Int) = {
    val fileContent = new StringBuilder
    var count = 0
    var ze: Option[ZipEntry] = None   
    try {
      do {
        ze = Option(zipStream.getNextEntry)
        ze.foreach { 
          ze => if (ze.getName.endsWith("java") && !ze.isDirectory) {
          val fileName = ze.getName
          fileContent.append(" ")
          fileContent.append(readContent(zipStream))
          // val in:Reader = new StringReader(readContent(zipStream))
          // try {
          //   val cu = JavaParser.parse(in, false)
          //   fileContent.append(" ")
          //   fileContent.append(cu.toString())
          // }
          // catch {
          //   case t: Throwable => log.error("Lexical error in next entry", t) // log and suppress 
          //   count = count + 1 
          //   fileContent.append(" ") 
          // }
          // in.close()
          } 
        }
      } while (ze.isDefined)
    } catch {
      case ex: Exception => log.error("Exception reading next entry {}", ex)
    }
    (fileContent.toString,count)
  }

  def readContent(stream: ZipInputStream): String = {
    val output = new ByteArrayOutputStream()
    var data: Int = 0
    do {
      data = stream.read()
      if (data != -1) output.write(data)
    } while (data != -1)
    val kmlBytes = output.toByteArray
    output.close()
    new String(kmlBytes, "utf-8").trim
  }

  def listAllFiles(dir: String): Array[File] = new File(dir).listFiles
}

