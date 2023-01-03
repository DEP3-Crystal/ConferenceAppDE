package com.crystal.jobs.utils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class EmailTransform {
    private static EmailTransform INSTANCE;
    private EmailTransform(){

    }
    public static synchronized EmailTransform getInstance(){
        if(INSTANCE == null){
            INSTANCE = new EmailTransform();
        }
        return INSTANCE;
    }

    public String mailTransform(String stringToReplace, String replacement,String path){
        try (Stream<String> lines = Files.lines(Path.of(path))) {
            return lines.map(line -> {
                if (line.contains(stringToReplace)) {
                    return line.replace(stringToReplace, replacement);
                }else {
                    return line;
                }
            }).collect(Collectors.joining("\n"));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
