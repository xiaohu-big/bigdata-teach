package com.wyh.flink_study.java.flinkTable;

public class WC {
    private String word;
    private Long Frequency;

    public WC() {
    }

    public WC(String word, Long frequency) {
        this.word = word;
        Frequency = frequency;
    }

    public String getWord() {
        return word;
    }

    public void setWord(String word) {
        this.word = word;
    }

    public Long getFrequency() {
        return Frequency;
    }

    public void setFrequency(Long frequency) {
        Frequency = frequency;
    }

    @Override
    public String toString() {
        return "WC{" +
                "word='" + word + '\'' +
                ", Frequency=" + Frequency +
                '}';
    }
}
