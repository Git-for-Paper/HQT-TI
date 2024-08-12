package rules;

import java.util.Stack;

public class StackwithInt {
    private Long nid;
    private Integer integer;


    public static void push(Stack<StackwithInt> stack, Long nid, Integer integer) {
        StackwithInt item = new StackwithInt(nid, integer);
        stack.push(item);
    }

    public static StackwithInt pop(Stack<StackwithInt> stack) {
        if (!stack.isEmpty()) {
            return stack.pop();
        } else {
            return null;
        }
    }
    public StackwithInt(Long nid, Integer integer) {
        this.nid = nid;
        this.integer = integer;
    }

    public Long getNid() {
        return nid;
    }

    public void setNid(Long nid) {
        this.nid = nid;
    }

    public Integer getInteger() {
        return integer;
    }

    public void setInteger(Integer integer) {
        this.integer = integer;
    }
}
