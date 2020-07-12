package com.tnpacs.dicoogle.mongoplugin.utils;

import com.mongodb.client.model.Filters;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.*;
import org.bson.BsonDocument;
import org.bson.conversions.Bson;
import org.dcm4che2.data.Tag;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class LuceneQueryToMongoQueryConverter {
    private final Query query;

    public LuceneQueryToMongoQueryConverter(Query query) {
        this.query = query;
    }

    public Bson convert() {
        return convert(query);
    }

    private Bson convert(Query query) {
        if (query instanceof BooleanQuery) {
            return convertBooleanQuery((BooleanQuery) query);
        } else if (query instanceof PrefixQuery) {
            return convertPrefixQuery((PrefixQuery) query);
        } else if (query instanceof PhraseQuery) {
            return convertPhraseQuery((PhraseQuery) query);
        } else if (query instanceof TermQuery) {
            return convertTermQuery((TermQuery) query);
        } else {
            return new BsonDocument();
        }
    }

    private Bson convertBooleanQuery(BooleanQuery query) {
        List<BooleanClause> clauses = query.clauses();
        List<Bson> convertedClauses = clauses.stream()
                .map(booleanClause -> convert(booleanClause.getQuery()))
                .collect(Collectors.toList());

        Bson result;
        BooleanClause.Occur occur = clauses.get(0).getOccur();
        if (occur == BooleanClause.Occur.MUST) {
            result = Filters.and(convertedClauses);
        } else if (occur == BooleanClause.Occur.SHOULD) {
            result = Filters.or(convertedClauses);
        } else if (occur == BooleanClause.Occur.MUST_NOT) {
            result = Filters.not(convertedClauses.get(0));
        } else {
            result = new BsonDocument();
        }
        return result;
    }

    private Bson convertPrefixQuery(PrefixQuery query) {
        Term term = query.getPrefix();
        String field = term.field();
        String prefix = term.text();
        Pattern pattern = Pattern.compile("^" + Pattern.quote(prefix), Pattern.CASE_INSENSITIVE);
        return Filters.regex(field, pattern);
    }

    private Bson convertPhraseQuery(PhraseQuery query) {
        String field = query.getField();
        String value = Arrays.stream(query.getTerms())
                .map(Term::text)
                .collect(Collectors.joining(" "));
        Pattern pattern = Pattern.compile("^" + Pattern.quote(value) + "$", Pattern.CASE_INSENSITIVE);
        return Filters.regex(field, pattern);
    }

    private Bson convertTermQuery(TermQuery query) {
        Term term = query.getTerm();
        String field = term.field();
        String value = term.text();

        // special case
        if (field.equals(Dictionary.getInstance().getName(Tag.StudyInstanceUID))
                || field.equals(Dictionary.getInstance().getName(Tag.SeriesInstanceUID))
                || field.equals(Dictionary.getInstance().getName(Tag.SOPInstanceUID))) {
            String[] uids = value.split("\\\\");
            List<Bson> clauses = Arrays.stream(uids)
                    .map(uid -> {
                        Pattern pattern = Pattern.compile("^" + Pattern.quote(uid) + "$", Pattern.CASE_INSENSITIVE);
                        return Filters.regex(field, pattern);
                    })
                    .collect(Collectors.toList());
            return Filters.or(clauses);
        }

        Pattern pattern = Pattern.compile("^" + Pattern.quote(value) + "$", Pattern.CASE_INSENSITIVE);
        return Filters.regex(field, pattern);
    }
}
