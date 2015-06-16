package pramod.OpenCalais;


import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.httpclient.methods.FileRequestEntity;
import org.apache.commons.httpclient.methods.PostMethod;
import org.openrdf.OpenRDFException;
import org.openrdf.model.Value;
import org.openrdf.query.BindingSet;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.query.TupleQueryResultHandler;
import org.openrdf.query.TupleQueryResultHandlerException;
import org.openrdf.query.resultio.sparqljson.SPARQLResultsJSONWriter;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.rio.RDFFormat;
import org.openrdf.sail.memory.MemoryStore;

/************************************************************
	- Simple Calais client to process file or files in a folder
	- Takes 2 arguments
		1. File or folder name to process
		2. Output folder name to store response from Calais
	- Please specify the correct web service location url for CALAIS_URL variable
	- Please adjust the values of different request parameters in the createPostMethod
	
**************************************************************/

public class InformationExtraction {

	private static final String CALAIS_URL = "http://api.opencalais.com/tag/rs/enrich";

    private File input;
    private File output;
    private HttpClient client;
    static RepositoryConnection con;

    private PostMethod createPostMethod() {

        PostMethod method = new PostMethod(CALAIS_URL);

        // Set mandatory parameters
        method.setRequestHeader("x-calais-licenseID", "m6pwcaxhs95j27ypb6asp834");

        // Set input content type
        //method.setRequestHeader("Content-Type", "text/xml; charset=UTF-8");
        //method.setRequestHeader("Content-Type", "text/html; charset=UTF-8");
        method.setRequestHeader("Content-Type", "text/raw; charset=UTF-8");

		// Set response/output format
        method.setRequestHeader("Accept", "xml/rdf");
        //method.setRequestHeader("Accept", "application/json");

        // Enable Social Tags processing
        method.setRequestHeader("enableMetadataType", "SocialTags,GenericRelations");

        return method;
    }

	private void run() {
		try {
            if (input.isFile()) {
                postFile(input, createPostMethod());
            } else if (input.isDirectory()) {
                System.out.println("working on all files in " + input.getAbsolutePath());
                for (File file : input.listFiles()) {
                    if (file.isFile())
                        postFile(file, createPostMethod());
                    else
                        System.out.println("skipping "+file.getAbsolutePath());
                }
            }
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

    private void doRequest(File file, PostMethod method) {
        try {
            int returnCode = client.executeMethod(method);
            if (returnCode == HttpStatus.SC_NOT_IMPLEMENTED) {
                System.err.println("The Post method is not implemented by this URI");
                // still consume the response body
                method.getResponseBodyAsString();
            } else if (returnCode == HttpStatus.SC_OK) {
                System.out.println("File post succeeded: " + file);
                saveResponse(file, method);
            } else {
                System.err.println("File post failed: " + file);
                System.err.println("Got code: " + returnCode);
                System.err.println("response: "+method.getResponseBodyAsString());
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            method.releaseConnection();
        }
    }

    private void saveResponse(File file, PostMethod method) throws IOException {
        PrintWriter writer = null;
        try {
            BufferedReader reader = new BufferedReader(new InputStreamReader(
                    method.getResponseBodyAsStream(), "UTF-8"));
            File out = new File(output, file.getName() + ".xml");
            writer = new PrintWriter(new BufferedWriter(new FileWriter(out)));
            String line;
            while ((line = reader.readLine()) != null) {
                writer.println(line);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (writer != null) try {writer.close();} catch (Exception ignored) {}
        }
    }

    private void postFile(File file, PostMethod method) throws IOException {
        method.setRequestEntity(new FileRequestEntity(file, null));
        doRequest(file, method);
    }


    public static void main(String[] args) throws IOException, RepositoryException {
        verifyArgs(args);
        InformationExtraction httpClientPost = new InformationExtraction();
        httpClientPost.input = new File(args[0]);
        httpClientPost.output = new File(args[1]);
        httpClientPost.client = new HttpClient();
        httpClientPost.client.getParams().setParameter("http.useragent", "Calais Rest Client");

        httpClientPost.run();
        
		Repository repo = new SailRepository(new MemoryStore());
		repo.initialize();	
		File file1 = new File("C:\\Users\\Pramod P. Setlur\\workspace\\Information Extraction\\output\\news1.txt.xml");
		File file2 = new File("C:\\Users\\Pramod P. Setlur\\workspace\\Information Extraction\\output\\news2.txt.xml");
		String baseURI = "http://example.org/example/local";
		try 
		{
		    con = repo.getConnection();
		   try 
		   {
		      con.add(file1, baseURI, RDFFormat.RDFXML);
		      con.add(file2, baseURI, RDFFormat.RDFXML);
		      
		      runQuery();
		   }
		   finally {
		      con.close();
		   }
		}
		catch (OpenRDFException e) 
		{
			e.printStackTrace();
		}
    }

    private static void runQuery() 
    {
    	String queryString = 	"PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>"+
								"PREFIX c: http://s.opencalais.com/1/type/em/e/Person"+
									
								"SELECT ?sub WHERE"+
								"{"+
									"?sub a ?y"+
								"}";
    	
    	String cityQuery = "PREFIX c: <http://s.opencalais.com/1/type/er/Geo/>"+
							"PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>"+
							"PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>"+
							"PREFIX pred: <http://s.opencalais.com/1/pred/>"+
							"Select ?z ?z"+
							"where"+
							"{?x pred:name ?z ."+
							"?x rdf:type c:City }";
    	
    	String personQuery =    "PREFIX c: <http://s.opencalais.com/1/type/er/Geo/>"+
								"PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>"+
								"PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>"+
								"PREFIX pred: <http://s.opencalais.com/1/pred/>"+
								"Select ?z"+
								"where"+
								"{?x pred:name ?z."+
								"?x rdf:type <http://s.opencalais.com/1/type/em/e/Person> }"; 
    	
    	String organizationQuery =      "PREFIX c: <http://s.opencalais.com/1/type/er/Geo/>"+
										"PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>"+
										"PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>"+
										"PREFIX pred: <http://s.opencalais.com/1/pred/>"+
										"Select ?x "+
										"where"+
										"{?x pred:name ?z."+
										"?x rdf:type <http://s.opencalais.com/1/type/em/e/Organization> }"; 
    	
 /*   	String genericRelationsQuery = "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>"+
										"PREFIX c: <http://s.opencalais.com/1/pred/>"+
										"Select ?subname ?objname ?verb "+
										"where"+
										"{?x rdf:type <http://s.opencalais.com/1/type/em/r/GenericRelations>."+
										"OPTIONAL{?x c:relationobject ?obj."+
										"?obj c:name ?objname.}}"+
										"?x c:relationsubject ?sub."+
										"?x c:verb ?verb."+  
										"?sub c:name ?subname.";*/
    	
    	String genericRelationsQuery = "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>"+
										"PREFIX c: <http://s.opencalais.com/1/pred/>"+
										"Select ?z ?subname ?verb"+
										"where"+
										"{?z rdf:type <http://s.opencalais.com/1/type/em/r/GenericRelations>."+
										
										"OPTIONAL{?z c:relationsubject ?sub."
										+ "?sub c:name ?subname.}"
												+ " ?z c:verb ?verb}";
										//"?z c:relationobject ?obj."+
										//"?sub c:name ?subname."+
										//"?z c:verb ?verb.}";		
    	
    	String queryString4="PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>"+
							"PREFIX c: <http://s.opencalais.com/1/pred/>"+
							"Select ?x ?sub ?subname ?objname ?obj ?verb"+
							"where"+
							"{?x rdf:type <http://s.opencalais.com/1/type/em/r/GenericRelations>."+
							"?x c:relationsubject ?sub."+
							"OPTIONAL {?x c:relationobject ?obj. ?sub c:name ?subname. ?obj c:name ?objname.}"+
							"?x c:verb ?verb.}"; 
										

    	
    	runSpecificQuery(personQuery);
    	System.out.println("\n\n------------------------\n\n");
    	runSpecificQuery(cityQuery);
    	System.out.println("\n\n------------------------\n\n");
    	runSpecificQuery(organizationQuery);
    	System.out.println("\n\n------------------------\n\n");
    	runSpecificQuery(queryString4);
		
	}


	private static void runSpecificQuery(String query)
	{
		try
		{
			TupleQuery tupleQuery = con.prepareTupleQuery(QueryLanguage.SPARQL, query);
			File allPersons = new File("C:\\Users\\Pramod P. Setlur\\Google Drive\\USC Subjects\\IIW\\Assignments\\HW12\\q1.txt");
			
			java.io.OutputStream os = new FileOutputStream(allPersons);
			TupleQueryResultHandler writer = new SPARQLResultsJSONWriter(os);
			TupleQueryResult result = tupleQuery.evaluate();
			
			try
			{
				while(result.hasNext())
				{
					BindingSet bindingSet = result.next();
					//Value a = bindingSet.getValue("sub");
					Value b = bindingSet.getValue("z");
					Value c =bindingSet.getValue("x");
					//Value d = bindingSet.getValue("obj");
					Value e =bindingSet.getValue("verb");
					
					/*Value sub = bindingSet.getValue("subname");
					Value objname = bindingSet.getValue("objname");
					Value obj = bindingSet.getValue("z");
					Value verb = bindingSet.getValue("verb");*/
					
					System.out.println(" "+" "+b+" "+c+" "+e);
				}
			}
			catch(Exception e)
			{
				e.printStackTrace();
			}
			finally
			{
				result.close();
			}
		}
		catch (MalformedQueryException e) 
		{
			e.printStackTrace();
		}  
		catch (QueryEvaluationException e) 
		{
		e.printStackTrace();
			} 
		catch (RepositoryException e) 
		{
			e.printStackTrace();
		} 
		catch (FileNotFoundException e) 
		{
			e.printStackTrace();
		} 
/*		catch (TupleQueryResultHandlerException e) 
		{
		e.printStackTrace();
		}*/
	}

	private static void verifyArgs(String[] args) {
        if (args.length==0) {
            usageError("no params supplied");
        } else if (args.length < 2) {
            usageError("2 params are required");
        } else {
            if (!new File(args[0]).exists())
                usageError("file " + args[0] + " doesn't exist");
            File outdir = new File(args[1]);
            if (!outdir.exists() && !outdir.mkdirs())
                usageError("couldn't create output dir");
        }
    }

    private static void usageError(String s) {
        System.err.println(s);
        System.err.println("Usage: java " + (new Object() { }.getClass().getEnclosingClass()).getName() + " input_dir output_dir");
        System.exit(-1);
    }

}
