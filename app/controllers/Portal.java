package controllers;

import play.Play;
import play.mvc.Result;
import play.mvc.Controller;

public class Portal extends Controller {

    public static Result archivesHtml(String projectid) {
        return ok(views.html.archives.render(projectid, request().username()));
    }
    
    public static Result monitor(){
    	String appver=Play.application().configuration().getString("app.version");
    	response().setContentType("text/plain");
    	
    	return ok("VERSION:"+appver+" STATUS:SUCCESS");
    }
}