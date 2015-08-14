/* HelloWorld.java
 */
package com.lsa.app;

public class CamelCase
{
	String splitCamelCase(String s) {
   		return s.replaceAll(
		      String.format("%s|%s|%s",
			 "(?<=[A-Z])(?=[A-Z][a-z])",
			 "(?<=[^A-Z])(?=[A-Z])",
			 "(?<=[A-Za-z])(?=[^A-Za-z])"
		      ),
		      " "
   		);
	}
	// public static void main(String[] args) {
	// 		System.out.println("Hello World!");
	// 		String[] tests = {
	// 			"lowercase",        // [lowercase]
	// 			"Class",            // [Class]
	// 			"MyClass",          // [My Class]
	// 			"HTML",             // [HTML]
	// 			"PDFLoader",        // [PDF Loader]
	// 			"AString",          // [A String]
	// 			"SimpleXMLParser",  // [Simple XML Parser]
	// 			"GL11Version",      // [GL 11 Version]
	// 			"99Bottles",        // [99 Bottles]
	// 			"May5",             // [May 5]
	// 			"BFG9000",          // [BFG 9000]
	// 			"caseCamel",
	// 		};
	// 	for (String test : tests) {
	// 		System.out.println("[" + splitCamelCase(test) + "]");
	// 	}
	// }
}
