package org.apache.hadoop.examples;


import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.rdd.func.MapFunc;
import org.apache.hadoop.rdd.func.TransformFunc;
import org.apache.hadoop.rdd.job.Stream;
import org.apache.hadoop.rdd.structure.Pair;
import org.apache.hadoop.rdd.structure.RDDPairList;
import org.apache.hadoop.rdd.io.save.Saver;
import org.apache.hadoop.rdd.transformation.Aggregate;
import org.apache.hadoop.rdd.types.Type;
import org.apache.hadoop.rdd.types.JsonType;
import org.apache.hadoop.rdd.types.writable.Writables;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;


public class RDDPageRank extends Configured implements Tool, Serializable {

    public static class PageRankScore {
        public float score;
        public float lastScore;
        public List<String> links;

        public PageRankScore() {
        }

        public PageRankScore(float score, float lastScore, Iterable<String> links) {
            this.score = score;
            this.lastScore = lastScore;
            this.links = Lists.newArrayList(links);
        }

        public PageRankScore next(float newScore) {
            return new PageRankScore(newScore, score, links);
        }

        public float propagatedScore() {
            return score / links.size();
        }

    }


    public static RDDPairList<String, PageRankScore> pageRank(RDDPairList<String, PageRankScore> input, final float dumping) {
        RDDPairList<String, Float> outbound = input.parallel(new TransformFunc<Pair<String, PageRankScore>, Pair<String, Float>>() {
            @Override
            public void process(Pair<String, PageRankScore> input, Saver<Pair<String, Float>> saver) {
                PageRankScore prScore = input.value();
                for (String link : prScore.links) {
                    saver.save(new Pair<>(link, prScore.propagatedScore()));
                }
            }
        }, Writables.tableOf(Writables.strings(), Writables.floats()));

        return input.cogroup(outbound).parallel(
                new MapFunc<Pair<String, Pair<Collection<PageRankScore>, Collection<Float>>>, Pair<String, PageRankScore>>() {
                    @Override
                    public Pair<String, PageRankScore> map(Pair<String, Pair<Collection<PageRankScore>, Collection<Float>>> input) {
                        PageRankScore prScore;
                        try{
                            prScore = Iterables.getOnlyElement(input.value().key());
                        } catch(Exception e){
                            prScore = new PageRankScore(1.0f, 0.0f, new ArrayList<>());
                        }
                        Collection<Float> propagatedScores = input.value().value();
                        float sum = 0.0f;
                        for (Float s : propagatedScores) {
                            sum += s;
                        }
                        return new Pair<>(input.key(), prScore.next(dumping + (1.0f - dumping) * sum));
                    }
                }, input.getPairListType());
    }

    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        conf.set("fs.defaultFS","file:///");
        conf.set("mapreduce.task.io.sort.mb","300");
        conf.set("mapreduce.map.java.opts","-Xmx3000m");
        conf.set("mapreduce.map.memory.mb","3096");
        conf.set("mapreduce.reduce.java.opts","-Xmx3000m");
        conf.set("mapreduce.reduce.memory.mb","3096");
        Stream pipeline = new Stream(RDDPageRank.class, conf);
        Type<PageRankScore> prType = JsonType.jsonString(PageRankScore.class);

        RDDPairList<String, PageRankScore> scores = pipeline.readTextFile("input")
                .parallel(new MapFunc<String, Pair<String, String>>() {
                    @Override
                    public Pair<String, String> map(String input) {
                        String[] urls = input.split("\\t");
                        return new Pair<>(urls[0], urls[1]);
                    }
                }, Writables.tableOf(Writables.strings(), Writables.strings())).groupByKey()
                .parallel(new MapFunc<Pair<String, Iterable<String>>, Pair<String, PageRankScore>>() {
                    @Override
                    public Pair<String, PageRankScore> map(Pair<String, Iterable<String>> input) {
                        return new Pair<>(input.key(), new PageRankScore(1.0f, 0.0f, input.value()));
                    }
                }, Writables.tableOf(Writables.strings(), prType));

        Float delta = 1.0f;
        while (delta > 0.1) {
            scores = pageRank(scores, 0.85f);
            scores.materialize().iterator();
            delta = Iterables.getFirst(Aggregate.max(scores.parallel(new MapFunc<Pair<String, PageRankScore>, Float>() {
                @Override
                public Float map(Pair<String, PageRankScore> input) {
                    PageRankScore prScore = input.value();
                    return Math.abs(prScore.score - prScore.lastScore);
                }
            }, Writables.floats())).materialize(), null);
        }
        pipeline.writeTextFile(scores, "output");
        pipeline.done();
        return 0;
    }
    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Configuration(), new RDDPageRank(), args);

    }
}