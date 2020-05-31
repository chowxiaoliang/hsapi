package bean;

/**
 * 用户评论
 */
public class AppComment {

    private int commentId;//评论表
    private int userId;//用户id
    private  int pCommentId;//父级评论id(为0则是一级评论,不为0则是回复)
    private String content;//评论内容
    private String addTime;//创建时间
    private int otherId;//评论的相关id
    private int praiseCount;//点赞数量
    private int replyCount;//回复数量

    public int getCommentId() {
        return commentId;
    }

    public void setCommentId(int commentId) {
        this.commentId = commentId;
    }

    public int getUserId() {
        return userId;
    }

    public void setUserId(int userId) {
        this.userId = userId;
    }

    public int getpCommentId() {
        return pCommentId;
    }

    public void setpCommentId(int pCommentId) {
        this.pCommentId = pCommentId;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }

    public String getAddTime() {
        return addTime;
    }

    public void setAddTime(String addTime) {
        this.addTime = addTime;
    }

    public int getOtherId() {
        return otherId;
    }

    public void setOtherId(int otherId) {
        this.otherId = otherId;
    }

    public int getPraiseCount() {
        return praiseCount;
    }

    public void setPraiseCount(int praiseCount) {
        this.praiseCount = praiseCount;
    }

    public int getReplyCount() {
        return replyCount;
    }

    public void setReplyCount(int replyCount) {
        this.replyCount = replyCount;
    }
}
