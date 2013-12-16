package org.wikipedia.miner.web.util.xjsfParameters;

import java.util.ArrayList;
import javax.servlet.http.HttpServletRequest;
import org.dmilne.xjsf.param.Parameter;

public class StringListParameter extends Parameter<String[]> {
    
    private final boolean caseSensitive ;
    
    
/**
     * Initialises a new (case sensitive) StringListParameter
     *
     * @param name the name of the parameter
     * @param description a short description of what this parameter does
     * @param defaultValue the value to use when requests do not specify a value
     * for this parameter (may be null)
     */
    public StringListParameter(String name, String description, String[] defaultValue) {
        super(name, description, defaultValue, "string list");
        caseSensitive = true;
    }

    /**
     * Initialises a new StringListParameter
     *
     * @param name the name of the parameter
     * @param description a short description of what this parameter does
     * @param defaultValue the value to use when requests do not specify a value
     * for this parameter (may be null)
     * @param caseSensitive true if you care about capitalisation of values
     */
    public StringListParameter(String name, String description, String[] defaultValue, boolean caseSensitive) {
        super(name, description, defaultValue, "string");
        this.caseSensitive = caseSensitive;
    }
    
    
    @Override
    public String getValueForDescription(String[] val) {

        if (val.length == 0) {
            return "empty list";
        }

        StringBuilder sb = new StringBuilder();
        for (String v : val) {
            sb.append(v);
            sb.append(",");
        }
        sb.deleteCharAt(sb.length() - 1);

        return sb.toString();
    }

    @Override
    public String[] getValue(HttpServletRequest request) throws IllegalArgumentException {

        String s = request.getParameter(getName());

        if (s == null) {
            return getDefaultValue();
        }

        ArrayList<String> values = new ArrayList<String>();
        for (String val : s.split("[,;:]")) {
            values.add((val.trim()));
        }
        return values.toArray(new String[values.size()]);
    }
}
