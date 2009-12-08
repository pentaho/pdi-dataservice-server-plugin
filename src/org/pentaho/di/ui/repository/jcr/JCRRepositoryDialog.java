package org.pentaho.di.ui.repository.jcr;

import org.eclipse.swt.SWT;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.ShellAdapter;
import org.eclipse.swt.events.ShellEvent;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.DirectoryDialog;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;
import org.pentaho.di.core.Const;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.repository.RepositoriesMeta;
import org.pentaho.di.repository.RepositoryMeta;
import org.pentaho.di.repository.jcr.JCRRepositoryLocation;
import org.pentaho.di.repository.jcr.JCRRepositoryMeta;
import org.pentaho.di.ui.core.PropsUI;
import org.pentaho.di.ui.core.gui.GUIResource;
import org.pentaho.di.ui.core.gui.WindowProperty;
import org.pentaho.di.ui.repository.dialog.RepositoryDialogInterface;
import org.pentaho.di.ui.trans.step.BaseStepDialog;

public class JCRRepositoryDialog implements RepositoryDialogInterface {
	private static Class<?> PKG = JCRRepositoryDialog.class; // for i18n purposes, needed by Translator2!!   $NON-NLS-1$

	private Label        wlURL;
	private Button       wbURL;
	private Text         wURL;

	private Label        wlManditoryComments;
	private Button       wManditoryComments;

	private Label        wlName;
	private Text         wName;

	private Label        wlDescription;
	private Text         wDescription;

	private Button wOK, wCancel;
    private Listener lsOK, lsCancel;

	private Display       display;
	private Shell         shell;
	private PropsUI         props;
	
	private JCRRepositoryMeta   input;
	// private RepositoriesMeta repositories;
	
	public JCRRepositoryDialog(Shell parent, int style, RepositoryMeta repositoryMeta, RepositoriesMeta repositoriesMeta)
	{
		this.display = parent.getDisplay();
		this.props=PropsUI.getInstance();
		this.input = (JCRRepositoryMeta) repositoryMeta;
		// this.repositories = repositoriesMeta;
		
		shell = new Shell(display, style | SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX | SWT.MIN );
		shell.setText(BaseMessages.getString(PKG, "JCRRepositoryDialog.Dialog.Main.Title")); //$NON-NLS-1$
	}

	public JCRRepositoryMeta open()
	{
		props.setLook(shell);

		FormLayout formLayout = new FormLayout ();
		formLayout.marginWidth  = Const.FORM_MARGIN;
		formLayout.marginHeight = Const.FORM_MARGIN;

		shell.setLayout(formLayout);
		shell.setImage(GUIResource.getInstance().getImageSpoon());
		shell.setText(BaseMessages.getString(PKG, "JCRRepositoryDialog.Dialog.Main.Title2")); //$NON-NLS-1$
		
		int middle = props.getMiddlePct();
		int margin = Const.MARGIN;

		// Add the connection buttons :
		wbURL = new Button(shell, SWT.PUSH);  wbURL.setText(BaseMessages.getString(PKG, "System.Button.Test")); //$NON-NLS-1$


		// ManditoryComments line
		wlManditoryComments=new Label(shell, SWT.RIGHT);
		wlManditoryComments.setText(BaseMessages.getString(PKG, "JCRRepositoryDialog.Label.ManditoryComments")); //$NON-NLS-1$
 		props.setLook(wlManditoryComments);
		FormData fdlManditoryComments = new FormData();
		fdlManditoryComments.left = new FormAttachment(0, 0);
		fdlManditoryComments.top  = new FormAttachment(wURL, margin);
		fdlManditoryComments.right= new FormAttachment(middle, -margin);
		wlManditoryComments.setLayoutData(fdlManditoryComments);
		wManditoryComments=new Button(shell, SWT.CHECK);
 		props.setLook(wManditoryComments);
		FormData fdManditoryComments = new FormData();
		fdManditoryComments.left = new FormAttachment(middle, 0);
		fdManditoryComments.top  = new FormAttachment(wURL, margin);
		fdManditoryComments.right= new FormAttachment(100, 0);
		wManditoryComments.setLayoutData(fdManditoryComments);

		
		// Add the listeners
		// New connection
		wbURL.addSelectionListener(new SelectionAdapter() 
			{
				public void widgetSelected(SelectionEvent arg0) 
				{
					DirectoryDialog dialog = new DirectoryDialog(shell, SWT.NONE);
					dialog.setText("Select root directory");
					dialog.setMessage("Select the repository root directory");
					String folder = dialog.open();
					if (folder!=null) {
						wURL.setText(folder);
					}
				}
			}
		);
	
		// Name line
		wlName=new Label(shell, SWT.RIGHT);
		wlName.setText(BaseMessages.getString(PKG, "JCRRepositoryDialog.Label.Name")); //$NON-NLS-1$
 		props.setLook(wlName);
		FormData fdlName = new FormData();
		fdlName.left = new FormAttachment(0, 0);
		fdlName.top  = new FormAttachment(wManditoryComments, margin*2);
		fdlName.right= new FormAttachment(middle, -margin);
		wlName.setLayoutData(fdlName);
		wName=new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
 		props.setLook(wName);
		FormData fdName = new FormData();
		fdName.left = new FormAttachment(middle, 0);
		fdName.top  = new FormAttachment(wManditoryComments, margin*2);
		fdName.right= new FormAttachment(100, 0);
		wName.setLayoutData(fdName);

		// Description line
		wlDescription=new Label(shell, SWT.RIGHT);
		wlDescription.setText(BaseMessages.getString(PKG, "JCRRepositoryDialog.Label.Description")); //$NON-NLS-1$
 		props.setLook(wlDescription);
		FormData fdlDescription = new FormData();
		fdlDescription.left = new FormAttachment(0, 0);
		fdlDescription.top  = new FormAttachment(wName, margin);
		fdlDescription.right= new FormAttachment(middle, -margin);
		wlDescription.setLayoutData(fdlDescription);
		wDescription=new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
 		props.setLook(wDescription);
		FormData fdDescription = new FormData();
		fdDescription.left = new FormAttachment(middle, 0);
		fdDescription.top  = new FormAttachment(wName, margin);
		fdDescription.right= new FormAttachment(100, 0);
		wDescription.setLayoutData(fdDescription);


		

		wOK=new Button(shell, SWT.PUSH);
		wOK.setText(BaseMessages.getString(PKG, "System.Button.OK")); //$NON-NLS-1$
		lsOK       = new Listener() { public void handleEvent(Event e) { ok();     } };
		wOK.addListener    (SWT.Selection, lsOK    );

		wCancel=new Button(shell, SWT.PUSH);
		wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel")); //$NON-NLS-1$
		lsCancel   = new Listener() { public void handleEvent(Event e) { cancel(); } };
		wCancel.addListener(SWT.Selection, lsCancel);

        BaseStepDialog.positionBottomButtons(shell, new Button[] { wOK, wCancel}, margin, wDescription);
		// Detect X or ALT-F4 or something that kills this window...
		shell.addShellListener(	new ShellAdapter() { public void shellClosed(ShellEvent e) { cancel(); } } );

		getData();

		BaseStepDialog.setSize(shell);

		shell.open();
		while (!shell.isDisposed())
		{
				if (!display.readAndDispatch()) display.sleep();
		}
		return input;
	}

	public void dispose()
	{
		props.setScreen(new WindowProperty(shell));
		shell.dispose();
	}
	
	/**
	 * Copy information from the meta-data input to the dialog fields.
	 */ 
	public void getData()
	{
		wName.setText(Const.NVL(input.getName(), ""));
		wDescription.setText(Const.NVL(input.getDescription(), ""));
		JCRRepositoryLocation location = input.getRepositoryLocation();
		if (location!=null) {
			wURL.setText(Const.NVL(location.getUrl(), ""));
		}else{
      wURL.setText("http://localhost:8585/jackrabbit/rmi"); // default jack rabbit URL
		}
		wManditoryComments.setSelection(input.isVersionCommentMandatory());
	}
	
	private void cancel()
	{
		input = null;
		dispose();
	}
	
	private void getInfo(JCRRepositoryMeta info)
	{
		info.setName(wName.getText());
		info.setDescription(wDescription.getText());
		info.setRepositoryLocation(new JCRRepositoryLocation(wURL.getText()));
		info.setVersionCommentMandatory(wManditoryComments.getSelection());
	}
	
	private void ok()
	{
		getInfo(input);
        
        if (input.getName()!=null && input.getName().length()>0)
        {
            dispose();
        }
        else
        {
            MessageBox box = new MessageBox(shell, SWT.ICON_ERROR | SWT.OK );
            box.setMessage(BaseMessages.getString(PKG, "JCRRepositoryDialog.Dialog.ErrorNoName.Message")); //$NON-NLS-1$
            box.setText(BaseMessages.getString(PKG, "JCRRepositoryDialog.Dialog.ErrorNoName.Title")); //$NON-NLS-1$
            box.open();
       }
	}
}
