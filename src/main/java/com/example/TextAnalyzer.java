package com.example; // <-- هذا هو السطر الجديد لحل المشكلة الأولى

import java.io.StringWriter;
import java.util.List;
import java.util.Properties;

import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.semgraph.SemanticGraph;
import edu.stanford.nlp.semgraph.SemanticGraphCoreAnnotations;
import edu.stanford.nlp.trees.Tree;
import edu.stanford.nlp.trees.TreeCoreAnnotations;
import edu.stanford.nlp.util.CoreMap;

/**
 * A standalone test class to demonstrate Stanford NLP analysis (POS,
 * Constituency, Dependency)
 * based on the v3.6.0 library specified in the assignment.
 */
public class TextAnalyzer {

    
    private static StanfordCoreNLP pipeline;

    public TextAnalyzer() {
        if (pipeline == null) {
            // Create a StanfordCoreNLP object, with POS tagging, lemmatization, NER,
            // parsing, and dependency parsing
            Properties props = new Properties();
            props.setProperty("annotators", "tokenize, ssplit, pos, lemma, ner, parse, depparse");

            // --- IMPORTANT ---
            // The first time you run this, it will be VERY SLOW (minutes)
            // because it has to download the required models.
            // After the first run, it will be much faster.
            System.out.println("Initializing Stanford NLP pipeline... (This may take a few minutes on first run)...");
            pipeline = new StanfordCoreNLP(props);
            System.out.println("Pipeline initialized!");
        }
    }

    /**
     * Performs Part-of-Speech (POS) tagging on a given text.
     *
     * @param text The input text.
     * @return A string showing the words and their POS tags.
     */
    public String analyzePOS(String text) {
        // Create an empty Annotation just with the given text
        Annotation document = new Annotation(text);

        // Run all Annotators on this text
        pipeline.annotate(document);

        // A StringWriter is a good way to build the output string
        StringWriter output = new StringWriter();

        // Get the sentences from the annotated document
        List<CoreMap> sentences = document.get(CoreAnnotations.SentencesAnnotation.class);

        for (CoreMap sentence : sentences) {
            // Iterate over all tokens in a sentence
            for (CoreLabel token : sentence.get(CoreAnnotations.TokensAnnotation.class)) {
                // Get the word
                String word = token.get(CoreAnnotations.TextAnnotation.class);
                // Get the POS tag
                String pos = token.get(CoreAnnotations.PartOfSpeechAnnotation.class);
                
                output.write(String.format("[%s/%s] ", word, pos));
            }
            output.write("\n");
        }
        return output.toString();
    }

    /**
     * Performs Constituency Parsing on a given text.
     *
     * @param text The input text.
     * @return A string showing the (S-expression) parse tree.
     */
    public String analyzeConstituency(String text) {
        Annotation document = new Annotation(text);
        pipeline.annotate(document);
        
        StringWriter output = new StringWriter();
        List<CoreMap> sentences = document.get(CoreAnnotations.SentencesAnnotation.class);

        for (CoreMap sentence : sentences) {
            // Get the constituency parse tree for the sentence
            Tree tree = sentence.get(TreeCoreAnnotations.TreeAnnotation.class);
            output.write("Sentence Parse Tree:\n");
            output.write(tree.toString());
            output.write("\n\n");
        }
        return output.toString();
    }

    /**
     * Performs Dependency Parsing on a given text.
     *
     * @param text The input text.
     * @return A string showing the dependency graph.
     */
    public String analyzeDependency(String text) {
        Annotation document = new Annotation(text);
        pipeline.annotate(document);
        
        StringWriter output = new StringWriter();
        List<CoreMap> sentences = document.get(CoreAnnotations.SentencesAnnotation.class);

        for (CoreMap sentence : sentences) {
            output.write("Sentence Dependency Graph:\n");
            // Get the dependency parse graph
            // This is the "collapsed CC-processed" dependencies, which is a standard, readable
            // format.
            SemanticGraph dependencies = sentence
                    .get(SemanticGraphCoreAnnotations.CollapsedCCProcessedDependenciesAnnotation.class);
            output.write(dependencies.toString(SemanticGraph.OutputFormat.LIST));
            output.write("\n\n");
        }
        return output.toString();
    }

    /**
     * Main method to run the analyzer locally.
     */
    public static void main(String[] args) {
        TextAnalyzer analyzer = new TextAnalyzer();

        String sampleText = "The quick brown fox jumps over the lazy dog. Stanford NLP is powerful.";

        System.out.println("--- 1. POS ANALYSIS ---");
        String posResult = analyzer.analyzePOS(sampleText);
        System.out.println(posResult);

        System.out.println("--- 2. CONSTITUENCY ANALYSIS ---");
        String constResult = analyzer.analyzeConstituency(sampleText);
        System.out.println(constResult);

        System.out.println("--- 3. DEPENDENCY ANALYSIS ---");
        String depResult = analyzer.analyzeDependency(sampleText);
        System.out.println(depResult);
    }
}